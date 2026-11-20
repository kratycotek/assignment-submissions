# Assignment 2.3: ETL pipeline

Airflow DAG `medallion` that loads five daily source files into the Gold model on Snowflake.

## Layout

| Path | What it is |
| --- | --- |
| `dags/medallion/` | The DAG, quality gates, pipelines, Snowflake adapter |
| `include/sql/` | DDL, Gold loads, seeds, reconciliation, manifests |
| `tests/` | Test suite |
| `evidence/` | Backfill screenshot, reconciliation output |
| `helpers/` | Source-data generator |
| `docker-compose.yaml` | Airflow stack, wired to this repo's `dags/`, `include/` and `data/` |

## Run

```bash
docker compose up -d          # UI on http://localhost:8080 (airflow / airflow)
uv run generate-data          # five days of CSVs into data/
uv run pytest tests
docker compose exec -T airflow-scheduler airflow backfill create --dag-id medallion \
  --from-date 2025-01-01 --to-date 2025-01-05 --max-active-runs 1
```

The compose file maps `dags/`, `include/` (read-only), `data/`, `logs/`, `config/` and `plugins/` into the container at the paths the DAG expects. `data/` is gitignored: run the generator before the first backfill.

Variables are optional; the DAG's defaults match the container paths (`data_root=/opt/airflow/data`, `sql_root=/opt/airflow/include/sql`). Set them in the UI (Admin -> Variables) if you want them explicit: `snowflake_conn_id`, `snowflake_schema`, `fs_conn_id`.

## What a run does

Sensors wait for the five files, then bootstrap (DDL and seeds), ingest into `STG_*`, silver into `TMP_*`, gold dimensions, then facts, then reconciliation.

- Each run takes its business date from the Airflow logical date; sensors look for `*_<date>.csv`.
- `max-active-runs` stays at 1; the dimension loads are date-ordered.

## Quality gates

Gates run in Python (`dags/medallion/gates/`). SQL is used only where a gate needs the whole table.

| Gate | Check | Action |
| --- | --- | --- |
| Q1 | Header matches expected columns | FAIL |
| Q2 | Row field count matches header | QUARANTINE |
| Q3 | At least `min_rows` rows survive Q2 | FAIL |
| Q4 | Values cast to their types | QUARANTINE |
| Q5 | Business keys present after cast | QUARANTINE or ALERT |
| Q6 | `sale_qty > 0`, `holding_price_amt > 0` | QUARANTINE or ALERT |
| Q7 | Dedupe on grain, first row wins | QUARANTINE |
| Q8 | `item_id` exists in the item master | QUARANTINE |
| Q9 | `store_id` exists in the store master | QUARANTINE |
| Q10 | A dimension version covers the date and supplies keys | QUARANTINE; `NO_KEY_DAY` is FAIL |
| Q11 | 33 reconciliation checks | EQ fails, LE alerts |

Quarantined rows go to `C_REJ_TABLE` with layer, gate, reason code and payload. Alerts go to `C_ALERT_TABLE`. Reconciliation ties source rows to staging, silver, rejects and facts; the day-3 defects (two null keys, one negative quantity, one duplicate line, one unknown item) are caught and counted, and the run continues.

## Reload policy

`C_LOAD_DATES` holds the latest loaded date per target; `C_PARAM` holds the reload window. Running an earlier date opens a window through the high-water mark, and every following date must then run in order. Fixing day 3 after days 4 and 5 succeeded means replaying 3, then 4, then 5.

## Tests

```bash
uv run pytest tests
```

52 tests: DAG parse, no cycles, retries, gate behaviour, control logic, pipeline against DuckDB.

## Limitations

- SQL steps log `NULL` row counts in `C_BATCH_LOG`; Python pipelines log exact counts.
- Dimensions load one date at a time, in order.
- Dedupe is within a batch only.
- Two batches for the same day overwrite each other's sales (assumed because of grain).
