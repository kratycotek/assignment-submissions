import re

from conftest import SQL_ROOT

EXPECTED_RECONCILE_CHECKS = {
    "bronze_item_master_rows",
    "silver_item_master_rows",
    "bronze_store_master_rows",
    "silver_store_master_rows",
    "bronze_pos_sales_rows",
    "silver_pos_sales_rows",
    "bronze_inventory_rows",
    "silver_inventory_rows",
    "bronze_price_rows",
    "silver_price_rows",
    "bronze_pos_amount",
    "silver_pos_amount",
    "gold_sales_rows",
    "gold_sales_amount",
    "gold_sales_vs_source",
    "gold_price_rows",
    "gold_inventory_rows",
    "unknown_fact_keys",
    "latest_sales_date",
    "latest_price_date",
    "latest_inventory_date",
    "fact_grain_dups_sales",
    "fact_grain_dups_price",
    "fact_grain_dups_inventory",
    "dim_window_overlap",
    "dim_window_bounds",
    "dim_current_versions",
    "dim_window_gaps",
    "reject_rate_item_master",
    "reject_rate_store_master",
    "reject_rate_pos_sales",
    "reject_rate_inventory",
    "reject_rate_price",
}

ALLOWED_TEMPLATES = {
    "params.batch_id",
    "ds",
    "source.item_master_rows",
    "source.store_master_rows",
    "source.pos_sales_rows",
    "source.inventory_rows",
    "source.price_rows",
    "source.pos_amount",
    "thresholds.unknown_fact_keys",
    "thresholds.reject_rate_pct",
    "tables.control.rej_table",
    "tables.control.alert_table",
    "tables.stg.item_master",
    "tables.stg.store_master",
    "tables.stg.pos_sales",
    "tables.stg.inventory",
    "tables.stg.price",
    "tables.tmp.item_master",
    "tables.tmp.store_master",
    "tables.tmp.pos_sales",
    "tables.tmp.inventory",
    "tables.tmp.price",
    "tables.dim.day",
    "tables.dim_item",
    "tables.dim_locn",
    "tables.dim.item",
    "tables.dim.locn",
    "tables.fct.sales",
    "tables.fct.price",
    "tables.fct.inventory",
}


def test_manifest_files_exist(manifest):
    listed = (
        manifest["bootstrap"] + manifest["gold_dimensions"] + manifest["gold_facts"]
    )
    listed += [manifest["reconcile"]]
    assert [name for name in listed if not (SQL_ROOT / name).is_file()] == []


def test_feeds_map_to_silver_tables_and_no_sql_silver_loads(manifest):
    feeds = {feed["name"] for feed in manifest["feeds"]}
    assert set(manifest["silver_masters"]) | set(manifest["silver_facts"]) == feeds
    assert not list((SQL_ROOT / "load").glob("tmp_*.sql"))
    assert not (SQL_ROOT / "load" / "gate_row_count.sql").exists()


def test_templates_use_only_known_variables():
    for path in SQL_ROOT.rglob("*.sql"):
        found = set(re.findall(r"\{\{\s*([\w.]+)\s*\}\}", path.read_text()))
        assert found <= ALLOWED_TEMPLATES, (path.name, found - ALLOWED_TEMPLATES)


def test_reconcile_check_set_is_pinned(manifest):
    text = (SQL_ROOT / manifest["reconcile"]).read_text()
    found = set(re.findall(r"SELECT '([a-z_]+)' AS CHECK_NM", text))
    assert found == EXPECTED_RECONCILE_CHECKS


def test_one_reject_table_only():
    text = "\n".join(path.read_text() for path in SQL_ROOT.rglob("*.sql"))
    assert "REJ_BRONZE" not in text
    assert "REJ_SILVER" not in text
    assert "REJ_GOLD" not in text


def test_load_files_have_no_comments():
    for path in (SQL_ROOT / "load").glob("*.sql"):
        if path.name == "reconcile.sql":
            continue
        commented = [
            line
            for line in path.read_text().splitlines()
            if line.lstrip().startswith("--")
        ]
        assert commented == [], path.name
