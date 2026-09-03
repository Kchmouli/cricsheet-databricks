# Cricsheet on Databricks — Architecture & Design Decisions

> Living document. We amend this as decisions are made. Each open decision carries a **proposed** default and an owner action.

_Last updated: 2026-08-17_

---

## 1. Goals

- Ingest Cricsheet ball-by-ball JSON and turn it into an analytics-ready dimensional model.
- **Incremental everywhere**: each run processes only new files and revised files — never a full truncate-and-reload.
- Minimise refresh time so the model updates in as little compute as possible (fair-usage quota matters on Free Edition).
- Land a Power BI–ready star with a delivery-grain fact, since most analysis is at ball level.

---

## 2. Source data (Cricsheet)

- One JSON file per match, format **v1.2.0**, sections: `meta`, `info`, `innings`.
- Match id is the **filename**, not a field inside the JSON — capture via `input_file_name()`.
- `meta.revision` increments when a match file is re-issued → this is our revision signal.
- Feeds:
  - `all_json.zip` (~22.5k matches) — one-off historical backfill.
  - `recently_added_2_json.zip` (previous 2 days) — the daily incremental feed. The 2-day window gives overlap so a failed run never drops a match.
- Caveat: 370 matches (Afghanistan / APL) are withheld — footnote any country-completeness claim.

---

## 3. Platform

- Databricks **Free Edition** (Community Edition retired 2026-01-01). Serverless-only, per-account quotas, fair-usage shutdown if exceeded.
- Scheduling via **Lakeflow Jobs** (this is why daily automation is now possible; old CE had no scheduler).
- Keep the daily job light; run the heavy backfill once.

---

## 4. Layer roles

| Layer | Role | Keys | Consumer |
|-------|------|------|----------|
| **Bronze** | Faithful raw capture as `VARIANT` (`data`) + typed lineage — queryable, schema-absorbing | `match_id`, `revision` | pipeline only |
| **Silver** | Conformed **atomic** integration layer — flattened, typed, deduped, full history. No surrogate keys. Tool-agnostic source of truth. | natural keys (`match_id`, `person_id`, …) | pipeline / ad-hoc |
| **Gold** | **Dimensional star** — surrogate/stable keys, `fact_ball` at delivery grain, conformed dims, optional aggregates. Presentation-shaped. | stable keys | Power BI |

**Why both silver and gold hold delivery data:** silver's atomic `deliveries` is the rebuildable truth (where cleaning/typing/dedup happen); gold's `fact_ball` is the star with keys tuned for BI. The duplication is intentional and cheap at this scale, and it means gold can be rebuilt or extended without re-ingesting.

**Bronze storage — decided: `VARIANT`.** The whole match document lands in one `VARIANT` column (`data`), with `match_id`, `revision`, `data_version`, and lineage as typed columns beside it. Chosen for long-term low maintenance: it captures the source faithfully and losslessly *and* stays queryable in SQL (`data:info:match_type::string`), and — critically — it **absorbs new optional fields with no pipeline change**. When Cricsheet adds a field, it's already in `data`, waiting to be pulled into silver whenever we choose.
- Rejected: raw JSON string (faithful but opaque — forces a parser change and can't be profiled in SQL); explicit struct schema in bronze (front-loads typing into the wrong layer, couples bronze to every source change, re-introduces the Test-vs-T20 union-schema problem).
- Fallback if `VARIANT` were unavailable: a parsed nested `struct` (keeps most queryability). Confirmed supported on Free Edition serverless (2026-08-17), so not needed.
- Explicit-schema discipline still happens — in **silver**, via `from_json` on the `VARIANT`, where flattening belongs.

---

## 5. Incremental load strategy (core principle)

**The unit of reprocessing is a whole match.** A revised file fully restates that match, so we replace all rows for that `match_id` rather than trying to patch individual balls.

1. **Bronze** — MERGE on `match_id`; upsert only when incoming `meta.revision` > stored revision (or the match is new). Idempotent; absorbs the 2-day overlap.
2. **Changed set** — the list of `match_id`s that were inserted/updated in bronze this run.
3. **Silver facts** (`deliveries`) — `replaceWhere match_id IN (changed set)`, insert fresh. Clustered by `match_id` so this is cheap.
4. **Silver dimensions** (players, teams, events) — MERGE-upsert any new/changed members that appear in the changed set. Never rebuilt wholesale.
5. **Gold `fact_ball`** — same `replaceWhere` on the changed set from silver.
6. **Gold dims** — MERGE-upsert.

**Deterministic keys, not sequences.** Use stable natural keys or hashes (`xxhash64`) for dimension keys so incremental loads never renumber anything:
- `dim_player` key = Cricsheet `person_id` (already a stable 8-char id — no surrogate needed).
- `dim_team` key = hash of normalised team name.
- `dim_series` key = hash of event name + season.
- `dim_calendar` key = `yyyymmdd` int.

This is what makes "load only what changed" safe — no central counter, no renumber-on-reload, no truncate.

**Storage:** Delta everywhere. Liquid clustering `CLUSTER BY match_id` on the fact (small data → no Hive partitioning). `match_id` is both the access key and the incremental-replace key.

---

## 6. Silver model (atomic, conformed, full history)

| Table | Grain | Source |
|-------|-------|--------|
| `deliveries` | 1 row per ball | explode `innings → overs → deliveries` (posexplode to keep innings no. + ball no.) |
| `matches` | 1 row per match | `info` |
| `players` | 1 row per person | union of all `registry` blocks |
| `teams` | 1 row per team | distinct teams across matches |
| `events` | 1 row per event(+season) | `info.event` |

**Decided (hybrid) — dismissal modelling.** Inline the primary dismissal on the delivery (`is_wicket`, `dismissal_kind`, `player_out_id`, `credited_to_bowler`) so delivery-grain wicket analysis stays on the fact. PLUS a `wicket_fielder` bridge (grain: wicket × fielder), linked by a `wicket_id` (hash of `match_id` + delivery coordinates + `player_out_id`), so fielding credit counts correctly across multi-fielder run-outs. No separate `fact_wicket` table. Rationale for the split fielder grain: a single dismissal can credit multiple fielders, so one fielder column would miscount two-fielder run-outs.
- Known limitation: the (very rare) two-wickets-on-one-ball case fills only the inline slot; the second wicket is not modelled.

**Decided — playing XI (`match_player`).** Grain match × team × player, from `info.players`. This is the **matches-played** grain: a player selected in the XI counts as a match played even if they never faced a ball or bowled.

**Matches vs innings semantic:**
- **Matches played** = distinct matches in `match_player` for a player (selection, not participation).
- **Innings** are derived from `fact_ball`, not from `match_player`: a **batting innings** exists only when the player appears as batter / is dismissed; a **bowling innings** only when they bowled ≥1 ball.
- So a 12th-man-style selection counts toward matches but never toward innings, strike rate, or economy.

---

## 7. Gold model (dimensional star)

### Dimensions
| Dim | Grain | Notes |
|-----|-------|-------|
| `dim_player` | 1 person | key = `person_id`. Role-playing: referenced by batter / bowler / non-striker / fielder. |
| `dim_team` | 1 team | `team_type` (international/club) as attribute. |
| `dim_series` | 1 event(+season) | needs an "unknown" member — bilateral matches may have no event. |
| `dim_venue` | 1 venue | promoted from a string on the fact; curated `canonical` unifies renames + near-duplicates (§15). |
| `dim_calendar` | 1 date | year/month/season/day-of-week. See date-attribution note below. |
| `dim_phase` *(proposed, see below)* | powerplay / middle / death | replaces the "overs" dimension idea. |

**Decided — "overs".** Over number is a **degenerate dimension** — store `over_number` (and `ball_number`) directly on `fact_ball`, no table. Phase analysis via a small `dim_phase` (powerplay / middle / death), with `phase_key` derived per ball from each innings' `powerplays` boundaries. No standalone `dim_overs`.

**Decided — role-playing player dimension.** One physical `dim_player`, referenced by batter / bowler / non-striker / fielder via multiple relationships in the Power BI model (one active, the rest inactive + `USERELATIONSHIP` in DAX). Single source of truth for player attributes.

**Date-attribution note:** deliveries carry no date; balls inherit the match start date. Multi-day Tests can't attribute a ball to a specific day from the JSON — documented limitation.

### Facts
| Fact | Grain | Status |
|------|-------|--------|
| `fact_ball` | 1 row per delivery | **Core.** FKs to all dims via stable keys; runs/extras/wicket flattened; `over_number`, `ball_number`, `innings_number`, `phase_key` as attributes. |
| `agg_batting` | player × match | **Deferred** — compute in DAX initially. |
| `agg_bowling` | player × match | **Deferred** — compute in DAX initially. |
| `agg_fielding` | player × match | **Deferred** — compute in DAX initially. |

**Decided — DAX-first, no materialised aggregates initially.** At ~6M ball rows, batting/bowling/fielding metrics compute fast in DAX directly on `fact_ball`, and each agg table is extra incremental maintenance. Pipeline stays *one fact + dims*.
- Fallback if a need appears (heavy career view, a metric with gnarly filter logic worth defining once): materialise at **player-match grain** and maintain incrementally with `MERGE` on `(match_id, player_id)` for the changed set — additive per match, so incremental stays clean.

---

## 8. Refresh flow (target)

```
daily job:
  1. download recently_added_2_json.zip -> Volume (Python task)
  2. MERGE into bronze (revision guard)  -> emit changed match_id set
  3. replaceWhere silver.deliveries on changed set; MERGE silver dims
  4. replaceWhere gold.fact_ball on changed set; MERGE gold dims
  (no aggregate tables to refresh while deferred)
```

No truncate-and-load at any layer. Compute scales with *what changed today*, not with total history.

---

## 9. Decisions log

1. Dismissals — **hybrid**: inline primary wicket on `fact_ball` + `wicket_fielder` bridge for fielding credit. ✅
2. Playing XI — **keep `match_player`**; matches-played vs innings semantic defined. ✅
3. "Overs" — **degenerate `over_number`** on fact + small `dim_phase`. ✅
4. Role-playing player dim — **one `dim_player`** + multiple relationships. ✅
5. Aggregates — **DAX-first**, no materialised agg tables initially. ✅

_Open decisions raised by profiling `1529269.json` (2026-08-17):_

6. **DRS reviews.** Delivery-level `review` object (by, umpire, decision, type). _Proposed: keep a `has_review` flag on `fact_ball` now; defer a dedicated `review` table unless DRS analysis is wanted._
7. **Impact player / replacements.** Delivery-level `replacements.match` (in/out/team/reason). _Proposed: small `replacement` table (match × in × out × reason); ensure `match_player` includes impact players (squad of 12+)._
8. **Officials.** `info.officials` (umpires, tv_umpires, reserve_umpires, match_referees). _Proposed: `match_official` bridge (match × role × person); keep them out of `dim_player`._
9. **Player of match multiplicity.** `player_of_match` is a list (can be >1). _Proposed: `match_award` bridge; take-first only if we want it flat on `dim_match`._
10. **Registry is a superset.** `registry.people` includes officials. _Decided: source `dim_player` from participants (XI + on-ball actors), resolve to id via registry; never dump the raw registry._

_Open decisions from the full-spec audit (2026-08-17):_

11. **Wicket modelling upgrade.** Two-wickets-on-one-ball is real. _Proposed: authoritative `wicket` table + `wicket_fielder` bridge, with `is_wicket`/`wicket_count` inline flags on the fact. Supersedes the earlier pure-inline hybrid._
12. **Edge-event coverage.** _Proposed: model the analytically-useful (`dim_innings`, `wicket`, `replacement`, `review`, `bowl_out`, `match_official`, `match_award`, `absent_hurt`); preserve rare housekeeping (`miscounted_overs`, `missing`) as raw. Confirm scope._
13. **Super-over treatment.** _Proposed: flag `is_super_over` and exclude from default batting/bowling aggregates; expose as an explicit filter. Confirm._

---

## 10. Changelog

- **2026-08-17** — Initial draft. Locked: star-at-gold, `fact_ball` delivery grain, incremental-only loads, full history retained in silver, deterministic keys. Aggregates deferred to DAX.
- **2026-08-17** — Locked all 5 parking-lot decisions: dismissals hybrid (inline + `wicket_fielder` bridge), `match_player` kept with matches-vs-innings semantic, degenerate over + `dim_phase`, single role-playing `dim_player`, DAX-first aggregates.
- **2026-08-17** — Profiled real match `1529269.json` (IPL 2026 T20). Added physical schemas for silver `deliveries` and `dim_match`, data-profile findings, and 5 new open decisions (DRS reviews, impact-player replacements, officials, player-of-match multiplicity, registry-superset).
- **2026-08-17** — Audited full v1.2.0 spec. Extended `deliveries` (is_super_over, actual_delivery, wicket_count) and `dim_match` (match_type_number, event_group/stage, toss_uncontested, bowl-out/method/missing). Added §13: `dim_innings`, `wicket`+`wicket_fielder`, `replacement`, `review`, `match_player`, `match_official`, `match_award`, `bowl_out`, `supersub`; type corrections (person_id string not char(8), event.group, target.overs notation); enumerations; forfeited/super-over/multi-innings explode rules. 3 new open decisions (11–13).
- **2026-08-17** — Validated against Test `1527273.json` (AUS v BAN, 4 innings). Multi-innings, null scheduled_overs, no powerplays, DRS reviews all confirmed; `actual_delivery` repeat-on-rebowl confirmed empirically. Added `review.type`. Started a validation log listing scenarios still unseen in samples.
- **2026-08-17** — Validated against The Hundred `1521261.json`. Surfaced that `match_type` is unreliable (The Hundred = `T20` with `balls_per_over: 5`) → added derived `format` column; confirmed consecutive-over bowling (10 straight) and balls-per-over ≠ entries-per-over; sharpened `dim_phase` to tuple comparison.
- **2026-08-17** — Validated 5 more files: super-over ties + `eliminator` (1529281, 1216512), `no result`/abandoned single innings (1527685), early chase (1426263), 2009/10 match (419150). Added derived `won_by_team` + season-form note; confirmed super-over exclusion (decision 13) and 8-char ids across 2009–2026. Remaining unseen: bowl_out, method, declared, forfeited, penalty_runs, absent_hurt, role replacements, missing, supersubs.
- **2026-08-17** — Closed out all dimension schemas (§14): `dim_player` + `player_name` bridge, `dim_team`, `dim_series`, `dim_calendar`, `dim_phase`. **Schema design complete** — next work is code, starting with bronze ingestion.
- **2026-08-17** — Added §15 curated enrichment layer (shared crosswalk pattern): `dim_player_enrichment` seed (dob, country, batting/bowling style, bat/bowl role, is_wicketkeeper, provenance); `team_alias` + franchise-key (history-preserving); promoted venue to a new `dim_venue` + `venue_alias` (history-preserving, handles renames + near-duplicates). All seeds optional/additive — pipeline builds `*_key` FKs from day one, canonical/enrichment resolve to null until seeded.
- **2026-08-17** — Started build. Bronze implemented: download `recently_added_2` → Volume → unzip → land one row per match as `VARIANT` (`data`) + typed `match_id`/`revision`/`data_version`/lineage, batch-dedup by revision, MERGE with revision guard, liquid clustering on `match_id`. `VARIANT` confirmed on Free Edition serverless; decision + trade-offs recorded in §4.
- **2026-08-17** — Built silver `dim_match` (VARIANT→StructType→typed pattern). **Corrected the Hundred classification** (per ICC/ACS: Hundred stats count as T20): replaced the overloaded `format` column with three orthogonal columns — `match_format` (statistical bucket; Hundred→T20, IT20/ODM/MDM kept distinct), `competition_variant` (Hundred visible for opt-in split), `is_international` (from team_type). Tier/competition slicing comes from `is_international` + `event_name`/`dim_series`, not from the format code.
- **2026-08-17** — Built silver `deliveries` (three-level `posexplode_outer` innings→overs→deliveries; `explode_outer` + null-filter guards forfeited/empty innings). Incremental via `whenNotMatchedBySourceDelete` scoped to batch match_ids (revised match fully replaces its balls). Verified on 72-match batch: 24,632 balls, `runs_total = runs_batter + runs_extras` on every row (0 mismatches). `batter_id`/`bowler_id`/`player_out_id` deferred to post-`dim_player` resolution.
- **2026-08-17** — Built `match_registry` (persisted (match_id, name, person_id) — reused later for officials/players/awards), `dim_player` (participants only; officials excluded via delivery-participant filter — 788 players from 1881 registry rows), and `player_name` bridge (canonical = most-used spelling). Resolved `*_id` onto `deliveries` via per-match `(match_id, name)` join (0 unresolved batters). `gender` pulled from bronze directly, added back to `dim_match`. Name-variant count 0 in this batch (expected — variants surface across seasons; will grow on full backfill).
- **2026-08-17** — Consolidated `deliveries` into a single flatten→resolve→write cell (removed the fragile two-pass split). Surrogate keys switched to `xxhash64` bigint. Built the four mechanical dims (`dim_team` 60, `dim_series` 12, `dim_calendar` 8, `dim_venue` 27) + empty `team_alias`/`venue_alias` seeds (canonical resolves to null until hand-populated). Built `dim_phase` (static lookup) + `ball_phase` (per-ball assignment via `(over,ball)` tuple comparison, `floor`/`*10` split — load-bearing single-digit-ball assumption noted). Verified `ball_phase = deliveries = 24,632` (every ball exactly one phase). Silver core complete.
- **2026-08-17** — Built high-value bridges (re-explode from bronze, resolve via `match_registry`): `wicket` (posexplode wickets → `wicket_index`, handles 2-on-a-ball; `is_bowler_wicket` from credited-kind set), `wicket_fielder` (fielder grain for multi-fielder run-outs), `match_player` (squad/matches-played grain), `dim_innings` (super_over/declared/forfeited/penalty/target). Cross-checks: `wicket` 1046 = `deliveries.is_wicket` 1046 (no dropped/dup dismissals, no 2-wicket balls this batch); `wicket_fielder` 752 < 1046 (gap = fielder-less dismissals, as designed); `match_player` 1585, `dim_innings` 145. Remaining silver: edge tables (`match_official`, `match_award`, `replacement`, `review`, `bowl_out`, `supersub`) — low priority.
- **2026-08-17** — **Built gold `fact_ball`** — the star fact. Assembly of `deliveries` + `ball_phase` + `dim_match` keys; FK hashes recomputed with the *same* expressions the dims use (deterministic keys line up with no lookup — must stay in sync if a dim hash changes). Derived measures `is_legal_ball` (wide/no-ball exclusion for SR/economy denominators) and `is_bowler_wicket` (recomputed from `dismissal_kind`, fast path; `wicket` table stays authoritative). Verified: `fact_ball = deliveries = 24,632`, and an end-to-end strike-rate leaderboard reads like real cricket (SR ~77–185).
- **2026-08-17** — Built gold dimension **views** over silver (low-maintenance, always in sync): `dim_player` (folds in `dim_player_enrichment` seed → `display_name`), `dim_team`/`dim_venue` (COALESCE canonical → `display_*`), `dim_series`, `dim_calendar`, `dim_phase`, `dim_match` (recomputes FK hashes to match the fact). Created the missing `dim_player_enrichment` seed. **Star is now self-contained in gold** — Power BI can consume gold alone.
- **2026-08-17** — **Bug found + fixed at source: `bowling_team` was NULL on every delivery.** Root cause: `deliveries` parsed `teams` from `data:teams` instead of `data:info:teams` (dropped the `info` level → null array → `array_except` yielded null). Symptom surfaced only in gold as `bowling_team_key` = constant `42` (xxhash64 of null). Two false leads first (an `F.col` wrapping theory, then `array_except`) — resolved by **confirming at the source** (checking the actual column values at each layer) rather than trusting the reasoning. Lesson for hardening: add not-null assertions on key columns at the end of each silver build to catch null-yielding path/column bugs at the boundary, not 3 layers downstream. Fixed path, rebuilt `deliveries` + `fact_ball`; star joins cleanly (24,632 through every dim, 0 dropped).
- **2026-08-18** — Version control: pushed all four notebooks + design doc to GitHub (Kchmouli/cricsheet-databricks, `main`). Wired a 4-task Lakeflow job (bronze→silver→gold→gold_dim_views) running from the Git Folder.
- **2026-08-18** — **Full backfill run** through the job on `all_json` (whole 2008–2026 archive). Scaled from 72-match sample to full history with no code changes: **22,681 matches, 11,475,038 deliveries, 13,306 players, 350k wickets**. `fact_ball` = `deliveries` = 11,475,038 (grain preserved at scale).
- **2026-08-18** — **Near-miss caught: job left `match_player`/`dim_innings` stale at 72 matches** while all other tables scaled to 22,681 (timestamps proved they weren't rewritten by the job — out-of-order cell execution suspected; job reported success despite the gap). Fixed by rerunning those cells against full bronze. **Added a silver coverage assertion** (fails the task if any key table's match count < bronze) as the permanent guard — this near-miss is exactly the silent-wrong-data failure the assertion prevents. Root cause of the job skip still to be diagnosed from the run log before scheduling daily.
- **2026-08-18** — **Pipeline validated against Cricinfo.** Kohli IPL-by-season reproduces published figures across 18 seasons: 2016=973, 2024=741, 2013=639, 2018=530, 2011=557, etc. — exact matches on runs, strike rate, sixes, match counts. One 5-run gap in 2020/21 traced to super-over inclusion (query didn't exclude `is_super_over`); confirms the flag works. **Full-history warehouse reproduces public records from an independent data path.**
- **2026-08-18** — Confirmed super-over exclusion: adding `NOT is_super_over` corrected 2020/21 (471→466) and 2013 (639→634) to match Cricinfo exactly, with all other seasons unchanged. **All 18 IPL seasons now match published figures.** Convention locked: **career/aggregate stat queries must filter `NOT is_super_over`** — bake this into every stat query and Power BI measure.
- **2026-08-18** — Full batting-card validation (Mat/Inns/NO/Runs/HS/Avg/BF/SR/100s/50s/4s/6s) against Kohli's IPL record surfaced three domain-correctness rules, now standard for all batting stats:
  1. **`is_ball_faced` ≠ `is_legal_ball`.** Balls faced excludes only wides (a batter *faces* a no-ball); legal ball excludes wides *and* no-balls (bowling/over concept). Using `is_legal_ball` for batting undercounted BF and overstated SR. Fix: add `is_ball_faced = (extra_wides = 0)` to `fact_ball`; batting uses `is_ball_faced`, bowling/economy uses `is_legal_ball`.
  2. **Boundaries need `non_boundary IS NOT TRUE`** — `runs_batter = 4/6` alone miscounts all-run fours as struck boundaries.
  3. **Matches ≠ Innings.** Mat = games in the squad (`match_player`); Inns = games he actually batted (from `fact_ball`). Mat ≥ Inns always.
  4. **An innings = faced ≥1 ball OR was dismissed.** A batter run out at the non-striker's end *without facing a ball* still counts as an innings (0 runs, out) — Cricinfo counts it. Found via KL Rahul IPL 2022 (match 1304099, run out innings 1, zero batting balls) showing 14 innings when it should be 15. Counting innings only from `fact_ball` batting rows **systematically undercounts innings** (and corrupts Average/NO) for every non-facing dismissal warehouse-wide. Fix: innings base = (batted-a-ball innings) UNION (dismissal innings from `wicket`), left-joining batting contributions. The `wicket` table — not just `fact_ball` — must drive innings/dismissal logic.
  5. **Not every `wicket` entry is a dismissal — `retired hurt`/`retired not out` are NOT outs.** The `wicket` array records "batter left the crease," and retired-hurt/not-out are innings but **not-outs** (the batter could return). Found via Rohit Sharma IPL 2026 (8 real dismissals + 1 retired hurt) showing 0 not-outs instead of 1. Fix: dismissal logic must filter `kind NOT IN ('retired hurt','retired not out')` — but **keep `retired out`** (a genuine, if rare, dismissal). The innings still counts (he batted); only `was_out` flips to false. `is_bowler_wicket` already excludes retirements via its fixed credited-kinds list, so bowling credit is unaffected.

**Voided-match exclusion:** match 1473495 (May 2025 IPL, abandoned mid-play during India–Pakistan tensions and *replayed* as a fresh fixture) is excluded from all records via a curated `silver.excluded_match` table (match_id + reason). Unlike ordinary rain no-results (which count as Mat), a voided-and-replayed match is treated as never having happened. Applied by anti-join at the gold `fact_ball` build and in the `Mat` (`match_player`) count — not per query.
- **2026-08-27** — Daily pipeline hardened: Bronze feed reverted to `recently_added_2_json.zip` (all-gender) with `shutil.rmtree(JSON_DIR)` before each extract (stale-file accumulation was making every run reprocess the whole archive → 40-min MERGE; now seconds). **Root-caused the vanishing `is_ball_faced`:** a stray, stale `fact_ball` rebuild cell lurking inside the Gold Dim Views notebook was overwriting the good fact table (without the column) every time the views ran — deleted it. Established: **views are create-once DDL, not daily data processing** — removed `gold_dim_views` from the daily schedule (bronze→silver→gold only); views reflect new data automatically and are re-run by hand only when a definition changes.
- **2026-08-27** — Built `gold.batting_innings` view (all six batting conventions baked in: super-over, is_ball_faced, non_boundary, innings=batted-or-dismissed, retired-hurt-not-dismissal; voided match inherited from fact_ball). Batting cards now one-liners over the view. Confirmed Genie available on Free Edition. Added **§16 analytics pattern catalogue** — 8 question patterns mapped to the views/columns each requires, the golden rule (Genie queries conventions-baked views, never raw fact_ball), bowling conventions spec, Genie config plan, and prioritised build backlog. This drives both the schema roadmap and the Genie instructions/example-queries.
  After these fixes, the full year-by-year batting card matches Cricinfo exactly across all 18 IPL seasons.
- **2026-08-17** — **Built gold `fact_ball`** — the star fact at delivery grain. Assembly: `deliveries` + `ball_phase` + `dim_match` keys; FK hashes recomputed with the *same* expressions the dims use (deterministic keys line up with no lookup — must change in both places if ever altered). Derived measures `is_legal_ball` (SR/economy denominator) and `is_bowler_wicket` (recomputed from `dismissal_kind`; `wicket` table stays authoritative for 2-on-a-ball + fielding). Verified: `fact_ball` = `deliveries` = 24,632 (grain preserved), 0 orphan date_keys, 0 null batter_ids on legal balls, and a full batting strike-rate leaderboard computed end-to-end through resolved ids → canonical names with sane cricket numbers. **Working dimensional warehouse: raw JSON → queryable star.**
- **2026-08-17** — **Built gold `fact_ball` — end-to-end pipeline working.** Delivery-grain star: joins `deliveries` + `ball_phase` + `dim_match` keys; FK hashes recomputed with the *same* expressions the dims used (deterministic-key contract — change in both places or FKs orphan); resolved `*_id`s carried through; derived `is_legal_ball` and `is_bowler_wicket` (recomputed from `dismissal_kind`, fast path; `wicket` table stays authoritative). Validated: grain preserved (24,632 = deliveries), 0 orphan date_keys, 0 null batter_id on legal balls, and a batting leaderboard with canonical names + sane strike rates (T20 hitters 175–185, accumulators 77–85). Raw JSON → queryable Power-BI-ready star confirmed.
- **2026-08-17** — Built gold `fact_ball` (delivery grain star). Assembly: `deliveries` + `ball_phase` + `dim_match` keys; FK hashes recomputed with the SAME expressions the dims use (deterministic keys line up without lookups — must change in both places if ever altered). Derived measures `is_legal_ball` and `is_bowler_wicket` (recomputed on fact for the fast path; `wicket` table stays authoritative). **End-to-end validated:** `fact_ball = deliveries = 24,632`, 0 orphan date keys, 0 null batter_id on legal balls, and a strike-rate leaderboard (runs/legal-balls via resolved ids → canonical names, super overs excluded) returns cricket-plausible values across formats. Pipeline works raw JSON → queryable star.
- **2026-08-17** — **Built gold `fact_ball`** (delivery-grain star). Assembly: `deliveries` + `ball_phase` + `dim_match` keys; FK hashes recomputed with the same expressions the dims use (deterministic-key contract — change in both places or FKs orphan); derived `is_legal_ball` and `is_bowler_wicket` inline (wicket table stays authoritative for 2-on-a-ball + fielding). Verified: `fact_ball` = `deliveries` = 24,632, 0 orphan date_keys, 0 null batter_id on legal balls. End-to-end batting strike-rate leaderboard computed off the star returns cricket-sane values. **Pipeline validated raw JSON → queryable star.**

---

## 11. Data-profile findings (from `1529269.json`, IPL 2026 T20)

- **meta:** `data_version` (str), `created` (date), `revision` (int).
- **info keys present:** balls_per_over, city, dates, event{name,match_number}, gender, match_type, officials{match_referees,reserve_umpires,tv_umpires,umpires}, outcome{winner,by}, overs, player_of_match (list), players{team→list}, registry{people}, season, team_type, teams (2), toss{decision,winner}, venue.
- **registry.people:** 29 entries, name→8-char id — **includes officials**, not just players.
- **players:** 12 per team (impact-player rule), not 11.
- **innings:** each has `team`, `overs`; optional `powerplays` (list of {from,to,type}); chasing innings has `target` {overs,runs}.
- **over:** 0-indexed; delivery count varies (first over had 7 entries due to a wide).
- **delivery keys seen:** batter, bowler, non_striker, runs{batter,extras,total}, and optionally extras, wickets, review, replacements.
- **extras kinds seen:** wides, legbyes, noballs (spec also: byes, penalty). Sparse dict — only present kinds appear.
- **wicket kinds seen:** caught, lbw, run out, retired hurt (spec has more).
- **fielders per wicket:** 0 (lbw, retired hurt), 1 (caught), 2 (the run out). Confirms multi-fielder bridge need.
- **no** two-wickets-on-one-ball case in this match (rare, as expected).
- **outcome shape:** here `winner` + `by{wickets}`. Draws/ties/no-result use `result`/`method`/`eliminator` — flatten defensively.

---

## 12. Physical schemas (in progress)

### Silver `deliveries` (atomic, 1 row per delivery entry)

| Column | Type | Null | Source / note |
|---|---|---|---|
| match_id | string | no | filename |
| innings_number | int | no | posexplode index, 1-based (Tests reach 3–4; super overs add more — never assume 2) |
| is_super_over | boolean | no | innings.super_over — keeps super-over balls out of normal stats |
| batting_team | string | no | innings.team |
| bowling_team | string | no | the other team in `teams` |
| over_number | int | no | over.over (0-indexed, kept as-is) |
| ball_seq | int | no | posexplode within over — includes extras; the safe ball key |
| actual_delivery | string | yes | delivery.actual_delivery (v1.2.0+, cricket notation e.g. `12.3`; **repeats on re-bowls — never key on it**; absent in 1.1.0 files) |
| batter | string | no | as recorded |
| bowler | string | no | as recorded |
| non_striker | string | no | as recorded |
| batter_id | string | no | resolved via match registry |
| bowler_id | string | no | resolved via match registry |
| non_striker_id | string | no | resolved via match registry |
| runs_batter | int | no | runs.batter |
| runs_extras | int | no | runs.extras |
| runs_total | int | no | runs.total |
| non_boundary | boolean | yes | runs.non_boundary (absent here) |
| extra_wides | int | no | default 0 |
| extra_noballs | int | no | default 0 |
| extra_byes | int | no | default 0 |
| extra_legbyes | int | no | default 0 |
| extra_penalty | int | no | default 0 |
| is_wicket | boolean | no | wickets present |
| wicket_count | int | no | len(wickets); usually 1, rarely 2 (wicket + retired hurt same ball) |
| dismissal_kind | string | yes | wickets[0].kind — inline convenience; authoritative detail in `wicket` table |
| player_out | string | yes | wickets[0].player_out — inline convenience |
| player_out_id | string | yes | resolved via registry |
| wicket_id | string | yes | hash(match_id, innings_number, over_number, ball_seq, player_out) |
| has_review | boolean | no | review present |
| has_replacement | boolean | no | replacements present |
| _source_file | string | no | lineage |
| _ingested_at | timestamp | no | lineage |
| revision | int | no | meta.revision, carried for lineage |

### Gold `fact_ball` (delivery grain, star)

Derived from silver `deliveries`. Drops raw name strings; keeps `*_id` as FKs to `dim_player`. Adds:

| Column | Type | Note |
|---|---|---|
| date_key | int | yyyymmdd, match start date |
| series_key | string | hash(event name + season) |
| batting_team_key | string | hash(team name) |
| bowling_team_key | string | hash(team name) |
| phase_key | string | derived from innings powerplay boundaries + over/ball |
| is_legal_ball | boolean | not a wide/no-ball — for balls-faced / economy denominators |
| is_bowler_wicket | boolean | dismissal_kind ∈ bowler-credited set (excludes run out, retired hurt, etc.) |

Degenerate on the fact: `over_number`, `ball_seq`, `innings_number`. Measures: runs_*, extra_*, is_wicket. Cluster by `match_id`.

### Gold `dim_match` (1 row per match)

| Column | Type | Null | Source |
|---|---|---|---|
| match_id | string | no | filename (PK) |
| match_type | string | no | info.match_type — raw Cricsheet code, never touched (`T20`, `IT20`, `ODI`, `ODM`, `Test`, `MDM`) |
| match_format | string | no | **derived** statistical bucket for Cricinfo-style aggregation — folds The Hundred (`T20` ∧ balls_per_over=5) into `T20`; keeps IT20/ODM/MDM distinct (they carry the domestic/intl signal) |
| competition_variant | string | yes | **derived** — `The Hundred` when applicable, else null; keeps the sub-format visible for the opt-in split |
| is_international | boolean | no | **derived** — `team_type = 'international'`; one-column filter for intl vs domestic/league |
| gender | string | no | info.gender |
| team_type | string | no | info.team_type |
| season | string | no | info.season |
| event_name | string | yes | info.event.name |
| match_type_number | int | yes | info.match_type_number (e.g. 2404th Test) — distinct from event number |
| event_match_number | int | yes | info.event.match_number |
| event_group | string | yes | info.event.group (int **or** string: `1`, `C`, `North`) |
| event_stage | string | yes | info.event.stage (e.g. `Final`, `Super 10`) |
| team_a | string | no | teams[0] |
| team_b | string | no | teams[1] |
| venue | string | no | info.venue |
| city | string | yes | info.city |
| start_date | date | no | min(dates) |
| end_date | date | no | max(dates) |
| balls_per_over | int | no | info.balls_per_over |
| scheduled_overs | int | yes | info.overs (null for Tests) |
| toss_winner | string | no | info.toss.winner |
| toss_decision | string | no | info.toss.decision |
| toss_uncontested | boolean | yes | info.toss.uncontested (v1.1.0; County 2016–19) |
| outcome_winner | string | yes | info.outcome.winner |
| outcome_result | string | yes | 'tie'/'draw'/'no result' when no winner |
| outcome_by_runs | int | yes | info.outcome.by.runs |
| outcome_by_wickets | int | yes | info.outcome.by.wickets |
| outcome_by_innings | boolean | yes | info.outcome.by.innings |
| outcome_method | string | yes | D/L, VJD, Awarded, '1st innings score', 'Lost fewer wickets' |
| outcome_eliminator | string | yes | super-over winner (tie) |
| outcome_bowl_out | string | yes | bowl-out winner (old T20 tie-break) |
| won_by_team | string | yes | **derived** — coalesce(winner, eliminator, bowl_out); null only for draw / no result |
| has_bowl_out | boolean | no | info.bowl_out present |
| has_missing_data | boolean | no | info.missing present |
| missing_raw | string | yes | info.missing preserved as JSON (not modelled) |
| data_version | string | no | meta.data_version |
| revision | int | no | meta.revision |

_Player-of-match handled via `match_award` bridge (see open decision 9), not inlined, since it's a list._

---

## 13. Full-spec audit (v1.2.0) — scenarios the T20 sample didn't show

Reviewed against the complete format spec. The `deliveries` and `dim_match` tables above were extended in place; the rest is captured here.

### New table — `dim_innings` (grain: match × innings)
Innings-level attributes belong here, not on the ball.

| Column | Type | Null | Source |
|---|---|---|---|
| match_id | string | no | |
| innings_number | int | no | array position, 1-based |
| batting_team | string | no | innings.team |
| is_super_over | boolean | no | innings.super_over |
| declared | boolean | no | innings.declared (Tests) |
| forfeited | boolean | no | innings.forfeited (v1.1.0) — **no `overs` array when true** |
| penalty_pre | int | no | innings.penalty_runs.pre, default 0 |
| penalty_post | int | no | innings.penalty_runs.post, default 0 |
| target_runs | int | yes | innings.target.runs |
| target_overs | decimal | yes | innings.target.overs (cricket notation, e.g. 35.1) |

Supporting: `innings_absent_hurt` (match × innings × player_id) from innings.absent_hurt — feeds matches-vs-innings; `miscounted_over` (match × innings × over × balls × umpire) — rare, optional. Innings `powerplays` feed `dim_phase` (below), not stored raw.

### Wicket handling — upgraded (open decision 11)
Two-wickets-on-one-ball is real (Sarfraz Ahmed stumped + Zulfiqar Babar retired hurt, PAK v AUS 2014); pure inline drops the second. **Proposed:** keep `is_wicket` + `wicket_count` inline on `fact_ball` for ball-grain filtering, make `wicket` authoritative.

`wicket` (grain: 1 per wicket entry): `wicket_id` = hash(match, innings, over, ball_seq, wicket_index); match/innings/over/ball_seq; `wicket_index` (0/1); `kind`; `player_out_id`; `is_bowler_wicket`.
`wicket_fielder` (grain: wicket × fielder): `wicket_id`, `fielder_id`, `fielder_seq` — handles multi-fielder run-outs.

### New delivery-event tables
- `replacement` (1 per entry): match_id + delivery coords, `type` (`match`|`role`), player_in_id, player_out_id (null for some role), team (null for role), reason, role (null for match). Reason vocab differs by type — see enums.
- `review` (1 per review): match_id + delivery coords, by_team, batter_id, umpire (null), decision (`struck down`|`upheld`), umpires_call (bool), `review_type` (review.type, e.g. `wicket` — present in data, **omitted from the spec table**). `has_review` flag stays on the fact.

### New match-level tables
- `match_player` (match × team × player_id) — squad, **including supersubs / concussion / covid / impact-player replacements** (the `players` list already contains them). Matches-played grain.
- `match_official` (match × role × person_id) — umpires, tv_umpires, reserve_umpires, match_referees. Kept out of `dim_player`.
- `match_award` (match × player_id) — player_of_match (list; usually 1).
- `bowl_out` (match × seq × bowler_id × outcome `hit`/`miss`) — rare T20 tie-break.
- `supersub` (match × team × player_id) — info.supersubs.

### Type corrections (important)
- **person_id is `string`, not `char(8)`.** Spec says 8-char hex, but also shows UUIDs. Empirically all samples 2009→2026 are 8-char; UUID form appears only in the spec example — keep `string` as insurance.
- **season** is a string in two forms: single-year (`2026`, calendar-year events like IPL) and split-year (`2020/21`, cross-year seasons). Derive `season_start_year` (int) for sorting.
- **event.group** may be int or string → `string`.
- **target.overs** is cricket notation (35.1 = 35 overs 1 ball), not a real decimal → keep raw, parse to (overs, balls) if needed.
- **gender / match_type** — do not hard-enum; spec says values may expand.
- **actual_delivery** repeats after wides/no-balls → not unique per over; `ball_seq` is the key.

### Enumerations
- **Wicket kinds:** bowled, caught, caught and bowled, lbw, stumped, run out, retired hurt, hit wicket, obstructing the field, hit the ball twice, handled the ball, timed out.
- **Bowler-credited (is_bowler_wicket = true):** bowled, caught, caught and bowled, lbw, stumped, hit wicket. Everything else = false.
- **Extras:** byes, legbyes, noballs, penalty, wides.
- **Powerplay types:** mandatory, batting, fielding.
- **outcome.method:** D/L, VJD, Awarded, 1st innings score, Lost fewer wickets.
- **replacement.match reasons:** concussion_substitute, covid_replacement, injury_substitute, national_callup, national_release, supersub, tactical_substitute, unknown.
- **replacement.role reasons:** excluded - excessive short-pitched deliveries, excluded - high full pitched balls, excluded - running on the pitch, injury, too many overs, unknown.
- **match_type:** Test, ODI, T20, IT20, ODM, MDM.

### Explode / edge-case rules
- **balls_per_over is not fixed, and not implied by match_type.** The Hundred is stored as `match_type: T20` with `balls_per_over: 5`. Never hardcode 6 or infer from match_type — always read `balls_per_over`. Entries per over still exceed `balls_per_over` when extras occur.
- **No over↔bowler alternation.** A bowler can bowl consecutive overs (The Hundred allows 10 balls straight; confirmed in `1521261.json`). Bowling aggregates must sum from the delivery grain — never assume a bowler change per over or alternating ends.
- **Forfeited innings have no `overs` array** — the explode must tolerate a missing `overs`. Super-over innings must be flagged so they're excluded from normal aggregates by default.
- **Multi-innings:** Tests up to 4, super overs add more — `innings_number` from posexplode handles any count.
- **`info.missing`** records known gaps (player_of_match, umpires, reviews, powerplays) — preserved as a flag + raw JSON on `dim_match`, not modelled.

### Deliberately preserved-as-raw (considered, not modelled)
`miscounted_overs`, `missing` detail, and future unknown fields kept verbatim in bronze (+ a raw struct column in silver) so nothing is lost without over-building tables for rare housekeeping.

### Validation log
- **`1529269.json`** — IPL 2026 T20 (v1.1.0). Two innings, impact-player 12-man squad, powerplays, target, multi-fielder run out.
- **`1527273.json`** — AUS v BAN Test 2026 (v1.2.0). 4 innings (team bats twice), null scheduled_overs, no powerplays, DRS reviews, byes/legbyes common, kinds bowled/caught/lbw/caught-and-bowled. **Empirically confirmed `actual_delivery` repeats on re-bowl (`0.2`,`0.2`)** → `ball_seq` is the key. Surfaced `review.type`.
- **`1521261.json`** — The Hundred 2026 (London Spirit v Welsh Fire). **`match_type: T20` but `balls_per_over: 5`** → match_type is unreliable for format; a bowler bowled 2 consecutive overs (10 straight); overs show 5–6 entries (extras). Drove the derived `format` column and the balls-per-over/no-alternation rules.
- **`1529281.json`** — IPL 2026 **tie decided by super over**. 4 innings (2 normal + 2 `super_over:true`), `outcome.result: tie` + `eliminator`; rare `obstructing the field` dismissal; impact-player `replacements.match`. Proves super-over handling + decision 13.
- **`1216512.json`** — IPL 2020/21, another tie + super over + eliminator. Season in split-year form `2020/21`.
- **`1426263.json`** — IPL 2024, normal 7-wkt win with early chase (2nd innings ends at 16 overs, partial final over).
- **`419150.json`** — IPL 2009/10 (oldest sample). Person ids **still 8-char**, not UUID — UUID form appears only in the spec example.
- **`1527685.json`** — IPL 2026 **no result** (rain). Single partial innings (4 overs, curtailed over + powerplay); `outcome: {result: no result}` only.
- Still unseen (spec-only): `bowl_out`, `method` (D/L etc.), `declared`, `forfeited`, `penalty_runs`, `absent_hurt`, `replacements.role`, `missing`, `supersubs`. (UUID person ids now look unlikely in current downloads — all samples 8-char.)

---

## 14. Dimension schemas

**`dim_player`** — grain: 1 person (participants only).
Built from the union of every match's `registry.people`, restricted to ids that appear as batter / bowler / non_striker / player_out / fielder, or in a `players` XI. Officials-only ids are excluded (their ids still resolve for `match_official` directly). Deliberately thin — Cricsheet gives only id + name, no DOB / country / batting style.

| Column | Type | Null | Source / derivation |
|---|---|---|---|
| person_id | string | no | registry id (PK) |
| canonical_name | string | no | most-used name across matches (tie-break: most recent) |
| gender | string | yes | from matches participated |
| first_match_date | date | no | min match start date |
| last_match_date | date | no | max match start date |

Bridge **`player_name`** — grain: person_id × name string: `person_id`, `name`, `usage_count`, `first_seen`, `last_seen`. Feeds `canonical_name` and lets you join on any historical spelling of a name.

**`dim_team`** — grain: 1 distinct team name.
Names are as-recorded, so franchise renames (Kings XI Punjab → Punjab Kings) appear as separate rows; unifying them needs a curated map, not derivable from data.

| Column | Type | Null | Source |
|---|---|---|---|
| team_key | string | no | hash(normalised name) (PK) |
| team_name | string | no | as recorded |
| team_type | string | yes | international / club (predominant) |
| canonical_team | string | yes | **curated** — unify renames (optional manual map) |
| first_match_date | date | no | min |
| last_match_date | date | no | max |

Caveat: national men's & women's sides share a name (e.g. "Australia") — slice by match `gender`, don't split the team row.

**`dim_series`** — grain: event × season.

| Column | Type | Null | Source / derivation |
|---|---|---|---|
| series_key | string | no | hash(event_name + season) (PK) |
| event_name | string | yes | info.event.name — `(no event)` member when absent (bilateral/domestic) |
| season | string | no | info.season |
| season_start_year | int | no | derived (`2020/21`→2020, `2026`→2026) |
| match_type | string | yes | predominant format |
| first_match_date | date | no | min |
| last_match_date | date | no | max |

`group` / `stage` stay on `dim_match` — they vary per match within a series.

**`dim_calendar`** — grain: 1 date.
A generated contiguous date spine from earliest to latest match date (every date, not just ones that occur), so Power BI time-intelligence works.

| Column | Type | Source |
|---|---|---|
| date_key | int | yyyymmdd (PK) |
| date | date | |
| year / month / day | int | |
| month_name / day_name | string | |
| quarter | int | |
| iso_week | int | |
| is_weekend | boolean | |

Deliveries inherit the match **start** date — multi-day Tests can't attribute a ball to a specific day (documented limitation).

**`dim_phase`** — small, authoritative from data.

| Column | Type | Source |
|---|---|---|
| phase_key | string | (PK) |
| phase_name | string | Powerplay / Non-powerplay / N-A |
| is_powerplay | boolean | |
| powerplay_type | string | mandatory / batting / fielding / null |

Members: PP-mandatory, PP-batting, PP-fielding, Non-powerplay, N/A (Tests — no powerplays).

Derivation (per delivery, at fact build): parse each innings powerplay `from` / `to` from cricket notation into `(over, ball)` integer tuples; a delivery at `(over_number, ball)` is inside that powerplay when `(from_over, from_ball) ≤ (over, ball) ≤ (to_over, to_ball)` — tuple compare, **never** float. Powerplays almost always span whole overs, so an over-level test is a fine default; the tuple form is what handles weather-curtailed powerplays ending mid-over (e.g. `0.1–3.4`, seen in `1527685.json`). Optional middle/death split is a format-specific convention layered on top (not in the data) — deferred unless wanted.

**Schema design status: complete.** All spec fields and every observed scenario have a home across `dim_match`, `dim_innings`, `fact_ball`, the five dims above, and the bridges/edge tables in §13. Remaining work is code (bronze ingestion first).

---

## 15. Curated enrichment layer (identity that survives renames)

Players, team renames, and venue renames are one pattern: the source records a **point-in-time label**, and we want a **stable identity** underneath it. Solved uniformly.

**Principle.** The pipeline always ingests names/venues exactly as recorded — raw values are never edited. Each affected dimension carries `canonical_*` / enrichment columns populated by a **left join to a small, hand-maintained seed table we own**. Auto-built dimension stays truthful to source; curated layer adds identity on top; the join is the last step, so enrichment can happen **anytime later without reprocessing history**. Unenriched rows simply show nulls, never drop out.

### `dim_player_enrichment` (seed, grain: 1 person)
Kept separate from `dim_player` so pipeline rebuilds never touch hand-entered data. Joined on `person_id`.

| Column | Type | Note |
|---|---|---|
| person_id | string | PK, matches registry id |
| full_name | string | enriched full name |
| country | string | static |
| dob | date | static |
| batting_style | string | RHB / LHB (Cricinfo attribute) |
| bowling_style | string | e.g. right-arm fast, slow left-arm orthodox |
| bat_role | string | top-order / middle-order / lower-order |
| bowl_role | string | pace / spin / none |
| is_wicketkeeper | boolean | keeper-batters carry two roles |
| source | string | provenance (e.g. Cricsheet Register / Cricinfo) |
| enriched_at | timestamp | provenance |

Notes: all nullable, populate when convenient. Role is **not truly static** (careers change) — treat these as primary/current labels; the *behavioural* truth (who actually bowled pace vs spin, batted where) is always derivable from `fact_ball`. Hook to pull this data later: the [Cricsheet Register](https://cricsheet.org/register/) maps `person_id` → Cricinfo/CricketArchive ids.

### `team_alias` (seed) + `dim_team.canonical`
No stable team id exists in the data, so renames need a crosswalk. **Decision: history-preserving** — keep a stable `franchise_key` plus the historical name, so you can show "RCB across all seasons" *and* what they were called in 2016.

`team_alias` (grain: team_name_as_recorded): `team_name`, `franchise_key`, `franchise_current_name`, `valid_from`, `valid_to`. `dim_team` gains `franchise_key` + `franchise_current_name` via join; `team_name` stays the as-recorded value. Example: `Royal Challengers Bangalore` and `Royal Challengers Bengaluru` → same `franchise_key`.

### `dim_venue` (NEW dimension) + `venue_alias` (seed)
Venue is promoted from a plain string on `dim_match` to its own dimension, because it has two distinct problems: genuine **renames** (sponsor changes) and raw **near-duplicates** (the string sometimes includes the city — `M Chinnaswamy Stadium` vs `M Chinnaswamy Stadium, Bengaluru`).

`dim_venue` (grain: 1 distinct venue string): `venue_key` = hash(normalised venue), `venue_name` (as recorded), `city`, `canonical_venue_key` (**curated**, unifies renames + near-duplicates), `canonical_venue_name`, first/last match date.
`dim_match` keeps `venue` (raw) and gains `venue_key` FK. **Decision: history-preserving** — canonical unifies for aggregate reporting, raw name retained for point-in-time.

`venue_alias` (seed): `venue_name` / `venue_key` → `canonical_venue_key`, `canonical_venue_name`. Hand-maintained.

### Build order note
All three seeds are optional and additive: the pipeline builds the auto dimensions and the FK/`*_key` columns from day one; the `canonical_*` and enrichment attributes are left joins that resolve to null until the seeds are populated. Nothing blocks on enrichment.

## 16. Analytics patterns & semantic layer (for AI/BI Genie)

The AI agent (Databricks Genie) does **not** answer from a fixed questionnaire — it generates SQL on the fly against a defined set of gold tables. Its reliability is bounded by two things: (a) the **semantic layer** it queries (conventions must be baked into views, never left for the LLM to apply), and (b) the **instructions + example queries** that teach it vocabulary and correct patterns. So the design task is not "list questions" but "enumerate the analytical *patterns*, and for each, build the view/column that makes it a simple, correct query."

**Golden rule:** Genie is pointed at **conventions-baked views** (`batting_innings`, `bowling_innings`, …), never at raw `fact_ball` for stats. On raw fact_ball the LLM would silently violate the seven conventions (super-over, is_ball_faced, non_boundary, matches≠innings, innings=batted-or-dismissed, retired-hurt, voided-match). The view is what makes the agent trustworthy.

### Pattern catalogue (question shape → what the model must expose)

| # | Pattern | Example question | Grain needed | View/concept | Status |
|---|---------|------------------|--------------|--------------|--------|
| 1 | Batting totals | "Kohli runs in 2016", "career avg", "50s/100s by season" | player × innings | `batting_innings` | ✅ built |
| 2 | Bowling totals | "Bumrah wickets 2023", "economy by season", "BBI/BBM" | player × bowling-innings | `bowling_innings` | ⬜ to build |
| 3 | Phase performance | "Kohli powerplay SR over years", "death-overs economy" | player × season × phase | `batting_by_phase` / `bowling_by_phase` + **death/middle classification** | ⬜ to build (needs 7th convention) |
| 4 | Batter-vs-bowler matchup | "Kohli vs Starc in Tests" | batter × bowler (× format) | `batting_vs_bowler` (fact_ball has both ids) | ⬜ to build |
| 5 | Outcome / impact | "how did Kohli's SR affect RCB win%" | player × match + team result | `player_match_summary` | ⬜ to build |
| 6 | Venue / geography | "Kohli at Chinnaswamy", "India vs overseas" | + venue country / home-away | `venue_country` enrichment + home/away derivation | ⬜ to build (enrichment) |
| 7 | Leaderboards | "top run-scorers IPL 2023", "best death economy" | any view above, no player filter | (free once views exist) | partial |
| 8 | Comparisons | "compare Kohli & Rohit in death overs" | same as underlying pattern, 2+ subjects | (Genie handles multi-subject; view handles correctness) | partial |

### The seven (now) domain conventions — must live in views, applied once
1. Career/aggregate stats exclude super-overs (`NOT is_super_over`).
2. Balls faced = `is_ball_faced` (wides only) — a no-ball IS faced; ≠ `is_legal_ball` (bowling).
3. Boundaries need `non_boundary IS NOT TRUE` (all-run fours are not boundaries).
4. Matches (`match_player` squad) ≠ Innings (batted from fact_ball). Mat ≥ Inns.
5. An innings = faced ≥1 ball OR was dismissed (catches run-out-without-facing).
6. `retired hurt`/`retired not out` are NOT dismissals (are not-outs); keep `retired out`.
7. **Death/middle overs are a defined convention, not in source** — death = overs 16-20 (`over_number >= 15`) for T20; must be encoded (powerplay IS authoritative in source, death/middle are NOT). *This one is still to be added to the model.*
Plus voided-match exclusion (`silver.excluded_match`, anti-joined at fact build).

### Bowling conventions (for `bowling_innings`)
- Wickets = `is_bowler_wicket` only (excludes run-out, retirements, obstruction).
- Balls bowled / overs = `is_legal_ball` (excludes wides AND no-balls); overs = legal_balls / 6.
- Runs conceded = `runs_batter + extra_wides + extra_noballs + extra_penalty` — **excludes byes & leg-byes** (keeper/batter fault, not bowler's).
- Economy = 6 × runs_conceded / legal_balls; Average = runs_conceded / wickets; SR = legal_balls / wickets.
- BBI = best (wickets desc, runs asc) over an innings; BBM = same over a match (BBM=BBI in T20, single innings); 4w/5w = innings hauls; 10w = match (Tests only).

### Genie space configuration (target)
- **Scope:** `batting_innings`, `bowling_innings`, phase views, dim views — NOT raw `fact_ball` for stats.
- **Instructions:** encode vocabulary (player names = initials+surname, e.g. `V Kohli`), the conventions above, "use the innings views not fact_ball", phase definitions.
- **Example (trusted) queries:** one validated query per pattern (the Kohli batting card, by-tournament breakdown, etc.) — teaches Genie the join/convention patterns to generalise from.

### Build backlog (priority order)
1. `bowling_innings` — unlocks pattern 2 (bowling), completes both-innings coverage.
2. Phase classification (`game_phase`: powerplay/middle/death, format-aware) + `batting_by_phase`/`bowling_by_phase` — unlocks pattern 3.
3. `player_match_summary` (performance + team result) — unlocks pattern 5 (impact).
4. `venue_country` enrichment + home/away — unlocks pattern 6 (overseas).
5. `batting_vs_bowler` — unlocks pattern 4 (matchup).
Each is a conventions-baked view/seed; each makes a category of questions Genie-reliable. Launch Genie after 1-2, expand iteratively.
