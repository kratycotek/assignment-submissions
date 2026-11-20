# Module 2: Retail Data Warehousing

Kritan Rijal. Three assignments, one pipeline.

- [`./model/`](./model/): 2.1, ER diagram, `create_tables.sql`, grain and SCD notes
- [`./architecture/`](./architecture/): 2.2, medallion diagram, layer inventory, gate and reload contracts
- [`./dags/`](./dags/): 2.3, the Airflow DAG package
- [`./include/`](./include/): 2.3, SQL (DDL, loads, seeds, reconciliation) and manifests
- [`./tests/`](./tests/): 2.3, test suite
- [`./evidence/`](./evidence/): 2.3, backfill screenshot and reconciliation output
- [`./helpers/`](./helpers/): 2.3, source-data generator
- [`./README_2_3.md`](./README_2_3.md): 2.3, how to run, how to backfill, gate contract, limitations

Run the tests: `uv run pytest tests`
