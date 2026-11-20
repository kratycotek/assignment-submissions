# Architecture Notes: Retail DW ETL

Diagram: [`architecture.mmd`](./architecture.mmd), rendered to `.svg` and `.png`.

## Inputs

Five daily files per batch. The fifth, `price_master`, is our addition: `FCT_PRICE` cannot be derived from the dimensions or from POS.

| File | Feed type | Bronze table | Gold targets |
| --- | --- | --- | --- |
| `item_master_YYYYMMDD.csv` | Snapshot | `STG_ITEM_MASTER` | `DIM_ITEM`, `DIM_CLASS` |
| `store_master_YYYYMMDD.csv` | Snapshot | `STG_STORE_MASTER` | `DIM_LOCN`, `DIM_DISTRICT` |
| `pos_sales_YYYYMMDD.csv` | Transactions | `STG_POS_SALES` | `FCT_SALES` |
| `inventory_snapshot_YYYYMMDD.csv` | Delta | `STG_INVENTORY` | `FCT_INVENTORY` |
| `price_master_YYYYMMDD.csv` | Snapshot | `STG_PRICE` | `FCT_PRICE` |

Inventory is a delta: an empty file is a valid day, so the row-count gate does not apply to it. It must still arrive.

## Layers

| Layer | Prefix | Holds |
| --- | --- | --- |
| Bronze | `STG_` | Raw text as landed, plus batch metadata. No business rules. |
| Silver | `TMP_` | Typed, checked, deduplicated, batch-scoped. |
| Gold | (none) | Star schema. |

All rejects from every layer go to one table, `C_REJ_TABLE`: `FAILED_LAYER_CD` (where it failed), `SOURCE_STAGE` (what it arrived from), `GATE_ID`, `REASON_CD`, `SRC_TABLE_NM`, `BATCH_ID`, `SRC_FILE_NM`, `PAYLOAD` (the row as received), `REC_INS_TS`. Each loader deletes its own batch and layer before writing, so a rerun replaces rejects instead of duplicating them.

## Quality gates

Gates are Python (`dags/medallion/gates/`). SQL is used only where a gate needs the whole table, through a `Warehouse` adapter that runs the same code on Snowflake and DuckDB.

Three actions. `FAIL`: write the reject, then abort the task. `QUARANTINE`: reject the row, continue. `ALERT`: keep the row, record it in `C_ALERT_TABLE`, continue. The first failing gate on a row wins.

| Gate | Check | Scope | Action |
| --- | --- | --- | --- |
| Q0 | File sensors wait for all five files, 600 s timeout | Run | times out |
| Q1 | Header equals expected columns, order and count included | File | FAIL |
| Q2 | Row field count matches the header | Raw batch | QUARANTINE |
| Q3 | At least `min_rows` rows survive Q2 (`min_rows = 0` for inventory) | Raw batch | FAIL |
| Q4 | Values cast to their types; blanks become null | Row | QUARANTINE |
| Q5 | Business keys present after cast | Row | QUARANTINE or ALERT |
| Q6 | `sale_qty > 0`, `holding_price_amt > 0` | Row | QUARANTINE or ALERT |
| Q7 | Dedupe on grain, first row wins; losers recorded | Batch | QUARANTINE |
| Q8 | `item_id` in the batch's item master | Batch, SQL | QUARANTINE |
| Q9 | `store_id` in the batch's store master | Batch, SQL | QUARANTINE |
| Q10 | One dimension version covers the date and supplies keys | Batch, SQL | `NO_KEY_DAY` FAIL; the rest QUARANTINE |
| Q11 | 33 reconciliation checks | Run | `EQ` fails, `LE` alerts |

Bronze reads the file, applies Q1, Q2, then Q3 on the survivors, and writes to `STG_*`. Silver reads `STG_*`, applies Q4 to Q10, and writes survivors to `TMP_*`. Q10 runs after the dimensions load, so it can resolve keys.

## Gold load

Per date, in order: SCD1 dims, SCD2 dims (ordering guard, then close the covering version, then insert the new one), facts, reconcile. Facts load last so a sale on a change date resolves to the new version.

`FCT_SALES` replaces its day. `FCT_PRICE` and `FCT_INVENTORY` are positional: upsert today's rows, then recompute every window from the data (a row is valid until the next date with a row for the same item and location; the latest stays open at `9999-12-31`). Windows come from the data, so load order does not matter.

## SCD and key resolution

A version covers `ds` when `EFF_FROM_DT <= ds < EFF_TO_DT`. The end bound is exclusive, so a sale on a change date gets the new version. Q10 runs one lookup per dimension and one code per row: `NO_KEY_DAY` (fatal), `AMBIGUOUS_ITEM`, `AMBIGUOUS_LOCN`, `NO_KEY_ITEM`, `NO_KEY_LOCN`.

Reconcile checks the dimension windows after facts load, all expected zero: `dim_window_overlap`, `dim_window_bounds`, `dim_current_versions`, `dim_window_gaps`.

## Control tables

| Table | Holds | Written by |
| --- | --- | --- |
| `C_BATCH_SCRIPTS` | Script registry, seeded at bootstrap | `seed_batch_scripts.sql` |
| `C_BATCH_LOG` | Per script per run: source/target table, status, counts, timing, failure reason | The DAG |
| `C_PARAM` | Run params per batch; reload window under `__CONTROL__` | The DAG |
| `C_REJ_TABLE` | Every rejected row | The writers |
| `C_ALERT_TABLE` | Alerted rows and `LE` threshold breaches | `AlertWriter` |
| `C_LOAD_DATES` | High-water mark per target; drives the reload policy | The DAG after each successful step |

## Reload policy

`C_LOAD_DATES` holds the high-water mark. Running an earlier date opens a window in `C_PARAM` from that date through the high-water mark, and every following date must then run in order. Correcting day 3 after days 4 and 5 succeeded means replaying 3, then 4, then 5. The wrong date fails with the expected date; retrying the date that just ran is allowed.

## Aggregate scope

`AGG_SALES_WEEKLY` is modelled but not loaded by this DAG. The daily pipeline is backfill-driven, so a weekly roll-up inside it would be recomputed on every replay and stays provisional until the week closes. It belongs in a separate weekly DAG reading the same warehouse. MERGE on `(ITEM_KY, LOCN_KY, WEEK_KY)`; `SRC_ROW_CNT` ties back to the week's fact rows.

## Retention

| Object | Retention |
| --- | --- |
| Source CSVs | 90 days, then archive (external job) |
| `STG_*`, `TMP_*` | 14 days of `BATCH_ID`, then delete |
| `C_REJ_TABLE`, `C_ALERT_TABLE` | Keep; quarantine audit |
| `C_BATCH_LOG` | 1 year, then archive |
| `C_PARAM` | Run params 90 days; `__CONTROL__` rows never, they hold the reload window |
| `C_LOAD_DATES`, `C_BATCH_SCRIPTS` | Keep, ~20 rows |
| `DIM_*`, `FCT_*` | Keep; SCD2 history |

Cutoffs are one SQL statement each, run outside the DAG, so retention cannot fail a load.

## Idempotency

Bronze and silver clear their batch and rejects before writing. `FCT_SALES` replaces its day. Positional facts upsert and recompute windows. Dimensions are no-ops on a same-date rerun. Running each date twice in order produces identical counts and totals.

## Known limitations

- Dimensions are not atomic: the SCD2 close and insert are separate statements. A failure between them shows up as `NO_KEY_*` rejects until repaired.
- Dimensions load in date order. The guard refuses out-of-order loads; it does not make them work.
- Dedupe is within a batch only.
- Master data must be in the same batch. A sale for an item loaded on an earlier day but absent today is rejected.
- Two batches for the same day overwrite each other's sales.
- Q1 is an exact match: a reordered header fails.
- A delta file that is empty by mistake cannot be detected; reconcile ties to what arrived.
- `CURR_FLG` is not checked against the `9999-12-31` sentinel.
- A dimension correction after facts load changes nothing already loaded.

## Evidence

In `evidence/`: the five-day grid, all green (`backfill_screenshot.png`), and the 2025-01-03 reconcile log showing all 33 checks tie and the day-3 reject-rate alert at 8.93% (`reconciliation_output.txt`). That log is the strongest single artifact: it shows the five defects quarantined and counted, and the run completing.
