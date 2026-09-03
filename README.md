# Cricsheet Cricket Lakehouse + Natural-Language Analytics Agent

An end-to-end **medallion lakehouse** on Databricks that turns Cricsheet's raw
ball-by-ball JSON (every recorded international and major-league match, 2008–2026)
into a validated analytics warehouse, exposed through a **natural-language agent**
(Databricks Genie).

It answers questions in plain English that no summary site publishes and that a
general-purpose AI cannot compute — because the answers require correct, ball-level
aggregation over 11.5 million deliveries:

> *"How did Virat Kohli's powerplay strike rate change over the years?"*
> *"Which bowler has dismissed Kohli the most in the IPL?"*
> *"How does Kohli fare against Bumrah in T20s?"*
> *"Is IPL 2026 Bumrah's worst season?"*

Every number is validated **exact against ESPNcricinfo** where a published reference exists.

---

## What it does

| Capability | Example question | Backed by |
|---|---|---|
| Batting & bowling career/season stats | "Kohli's IPL runs by season" | `batting_innings`, `bowling_innings` |
| Matches played, win % & impact | "Does his team win more when he scores fast?" | `player_match_summary` |
| Innings-phase analysis (powerplay/middle/death) | "Powerplay strike rate over the years" | `batting_by_phase`, `bowling_by_phase` |
| Batter-vs-bowler matchups (by competition) | "Kohli vs Bumrah in IPL" | `batter_vs_bowler` |

---

## Architecture

```
Cricsheet JSON (one file per match)
        │  download → Unity Catalog Volume
        ▼
┌─────────────┐   VARIANT capture: full document + typed match_id / revision /
│   BRONZE    │   lineage; MERGE with revision guard (idempotent, schema-absorbing)
└─────────────┘
        ▼
┌─────────────┐   Conformed atomic tables: deliveries (3-level explode),
│   SILVER    │   dim_match / dim_player / dim_team / dim_venue / dim_series,
│             │   ball_phase, wicket, match_player, dim_innings, match_registry
└─────────────┘   Deterministic xxhash64 keys; player identity resolved per-match
        ▼
┌─────────────┐   Star schema: fact_ball (delivery grain) + dimension views
│    GOLD     │   Semantic layer: conventions-baked views the agent queries
└─────────────┘
        ▼
   Databricks Genie  ──►  natural-language Q&A
```

- **Bronze** stores each match as a `VARIANT` (queryable semi-structured), so new
  source fields are absorbed without code changes.
- **Silver** is the conformed truth: an 11.5M-row `deliveries` table built by a
  three-level `posexplode` (innings → overs → deliveries), with player names
  resolved to stable ids via each match's own registry.
- **Gold** is a presentation star plus a **semantic layer** of views where every
  cricket convention is applied once, so downstream queries (and the AI agent)
  are correct by construction.

---

## The hard part: domain-correct statistics

Reproducing official cricket numbers exactly required encoding conventions that are
invisible until they bite. Each was found by validating a real player against
Cricinfo and refusing to accept a number that was "close":

1. **Super-overs excluded** from career/aggregate stats.
2. **Balls faced ≠ legal balls** — a batter faces a no-ball (batting excludes only
   wides); a bowler's over excludes wides *and* no-balls. Two different measures.
3. **Boundaries need a non-boundary guard** — an all-run four is not a struck boundary.
4. **Matches ≠ innings** — matches played come from the squad; innings from
   participation. (Found via a part-timer whose "matches" were being under-counted.)
5. **An innings = faced a ball OR was dismissed** — a batter run out without facing
   still has an innings. (Found via KL Rahul, IPL 2022.)
6. **`retired hurt` is not a dismissal** — it's a not-out. (Found via Rohit Sharma, 2026.)
7. **Death/middle overs are a convention, not source data** — powerplay comes from
   the source per-innings (handles shortened games, split ODI powerplays); death is
   defined (last 5 overs T20 / last 10 ODI), *flexed to innings length*, and takes
   precedence over a powerplay in the ODI overlap.

Plus a curated **voided-match exclusion** (an abandoned-and-replayed 2025 IPL game
treated as never having happened) and identity resolution for players recorded under
multiple name spellings across seasons.

These live in the gold views — never in ad-hoc SQL — which is what makes the
natural-language agent trustworthy rather than plausibly wrong.

---

## Repository layout

```
notebooks/
  Bronze.ipynb          # download → Volume → VARIANT land → MERGE
  Silver.ipynb          # explode + conform: deliveries, dims, bridges, phase
  Gold.ipynb            # fact_ball star (+ over_phase) + integrity/staleness checks
  Gold Dim Views.ipynb  # gold dimension views + semantic views (create-once DDL)
docs/
  design.md             # full design doc: architecture, decisions, conventions,
                        # validation log, and the debugging history
```

**Semantic views** (in `Gold Dim Views`): `batting_innings`, `bowling_innings`,
`player_match_summary`, `batting_by_phase`, `bowling_by_phase`, `batter_vs_bowler` —
each carrying Unity Catalog column comments so the agent understands the data.

---

## Pipeline & operations

- **Daily job** (`bronze → silver → gold`) runs from this repo on serverless compute.
  Views are **create-once DDL** — they reflect new data automatically and are re-run
  by hand only when a definition changes (a stray view-notebook rebuild of the fact
  table was once silently corrupting it — a good lesson in what belongs where).
- **Incremental & idempotent**: the reprocessing unit is a whole match; deterministic
  keys mean re-runs never renumber; a revision guard handles re-issued matches.
- **Data-quality guards**: a silver coverage assertion (every key table covers the
  same match count as bronze) and a gold staleness assertion catch "green but stale"
  runs — the failure mode that once let the warehouse silently fall a week behind.

---

## Tech

Databricks (Unity Catalog, serverless, Lakeflow Jobs, AI/BI Genie), PySpark, Delta
Lake, Spark SQL. Source data: [Cricsheet](https://cricsheet.org).

## Notes

- Data coverage follows Cricsheet — near-complete for internationals and major
  leagues from ~2008; sparser for older or obscure matches.
- The agent's *aggregations* are validated and reliable; its *interpretation* of
  results (e.g. framing a correlation as impact) is guided toward honesty but, like
  any text-to-SQL system, benefits from a critical read. Numbers are the product;
  editorialising is a draft.
