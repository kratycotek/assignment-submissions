import datetime as dt
import json
import re
from pathlib import Path

import pytest

try:
    import duckdb
    import sqlglot
except ImportError:  # pragma: no cover
    pytest.skip("duckdb and sqlglot are required", allow_module_level=True)
from medallion.gates import (
    BronzePipeline,
    FeedContext,
    FeedSpec,
    GateFileError,
    SilverPipeline,
    build_runner,
    load_tables,
)
from medallion.warehouse import DuckDbWarehouse

REPO = Path(__file__).resolve().parents[1]
SQL = REPO / "include" / "sql"
PIPELINE = json.loads((SQL / "pipeline.json").read_text())
SPECS = {raw["name"]: FeedSpec.from_dict(raw) for raw in PIPELINE["feeds"]}
TABLES = load_tables(SQL / "tables.yaml")


def run_ddl(con, text: str):
    text = re.sub(r"INTEGER IDENTITY\(1,1\) PRIMARY KEY", "INTEGER", text)
    text = text.replace("VARIANT", "JSON")
    for statement in [
        s.strip() for s in re.split(r";[ \t]*(?:\n|$)", text) if s.strip()
    ]:
        for translated in sqlglot.transpile(
            statement, read="snowflake", write="duckdb"
        ):
            con.execute(translated)


@pytest.fixture
def warehouse():
    con = duckdb.connect()
    for name in ("control.sql", "staging.sql", "silver.sql"):
        run_ddl(con, (SQL / "ddl" / name).read_text())
    con.execute("CREATE TABLE DIM_DAY (DAY_KY INTEGER, CALENDAR_DT DATE)")
    con.execute(
        "CREATE TABLE DIM_ITEM (ITEM_KY INTEGER, ITEM_ID INTEGER, "
        "EFF_FROM_DT DATE, EFF_TO_DT DATE, CURR_FLG BOOLEAN DEFAULT TRUE)"
    )
    con.execute(
        "CREATE TABLE DIM_LOCN (LOCN_KY INTEGER, LOCN_ID INTEGER, "
        "EFF_FROM_DT DATE, EFF_TO_DT DATE, CURR_FLG BOOLEAN DEFAULT TRUE)"
    )
    for i in range(10):
        con.execute(
            "INSERT INTO DIM_DAY VALUES (?, ?)", (i + 1, dt.date(2025, 1, 1 + i))
        )
        con.execute(
            "INSERT INTO DIM_ITEM (ITEM_KY, ITEM_ID, EFF_FROM_DT, EFF_TO_DT) "
            "VALUES (?, ?, DATE '2025-01-01', DATE '9999-12-31')",
            (100 + i, i + 1),
        )
    for s in range(5):
        con.execute(
            "INSERT INTO DIM_LOCN (LOCN_KY, LOCN_ID, EFF_FROM_DT, EFF_TO_DT) "
            "VALUES (?, ?, DATE '2025-01-01', DATE '9999-12-31')",
            (200 + s, s + 1),
        )
    yield DuckDbWarehouse(con)
    con.close()


def ctx(name: str, batch_id: str = "20250103") -> FeedContext:
    spec = SPECS[name]
    return FeedContext(
        batch_id=batch_id,
        business_dt=dt.date(2025, 1, 3),
        path=str(REPO / "data" / spec.file.format(ds=batch_id)),
        spec=spec,
    )


def run_feed(warehouse, name, batch_id="20250103", actions=None):
    BronzePipeline(warehouse, SPECS[name], TABLES).run(ctx(name, batch_id))
    runner = build_runner(warehouse, SPECS[name], actions, TABLES)
    return SilverPipeline(warehouse, SPECS[name], runner, TABLES).run(
        ctx(name, batch_id)
    )


def test_day_three_defects_are_quarantined(warehouse):
    run_feed(warehouse, "item_master")
    run_feed(warehouse, "store_master")
    result = run_feed(warehouse, "pos_sales")
    assert (result.rows_in, result.rows_out, result.rows_rejected) == (56, 51, 5)

    rejects = warehouse.query(
        "SELECT GATE_ID, REASON_CD FROM C_REJ_TABLE "
        "WHERE BATCH_ID = '20250103' ORDER BY GATE_ID"
    )
    silver = [r for r in rejects if r[0] in {"Q5", "Q6", "Q7", "Q8", "Q9"}]
    assert silver == [
        ("Q5", "NULL_KEY"),
        ("Q5", "NULL_KEY"),
        ("Q6", "NEG_QTY"),
        ("Q7", "DUP_LINE"),
        ("Q8", "UNKNOWN_ITEM"),
    ]
    kept = warehouse.query(
        "SELECT COUNT(*) FROM TMP_POS_SALES WHERE BATCH_ID = '20250103'"
    )[0][0]
    assert kept == 51


def test_gold_rejects_when_no_dimension_version(warehouse):
    run_feed(warehouse, "item_master")
    run_feed(warehouse, "store_master")
    warehouse.execute("DELETE FROM DIM_ITEM WHERE ITEM_ID = 3")
    run_feed(warehouse, "pos_sales")
    gold = warehouse.query(
        "SELECT REASON_CD, COUNT(*) FROM C_REJ_TABLE "
        "WHERE FAILED_LAYER_CD = 'GOLD' GROUP BY 1"
    )
    assert gold == [("NO_KEY_ITEM", gold[0][1])]
    assert gold[0][1] > 0
    src = warehouse.query(
        "SELECT DISTINCT SRC_TABLE_NM FROM C_REJ_TABLE WHERE FAILED_LAYER_CD = 'GOLD'"
    )
    assert src == [("TMP_POS_SALES",)]


def test_alert_action_records_alert_and_keeps_row(warehouse):
    run_feed(warehouse, "item_master")
    run_feed(warehouse, "store_master")
    run_feed(warehouse, "pos_sales", actions={"Q5": "ALERT"})
    alerts = warehouse.query(
        "SELECT GATE_ID, REASON_CD, COUNT(*) FROM C_ALERT_TABLE "
        "WHERE BATCH_ID = '20250103' GROUP BY 1, 2"
    )
    assert alerts == [("Q5", "NULL_KEY", 2)]
    kept = warehouse.query(
        "SELECT COUNT(*) FROM TMP_POS_SALES WHERE BATCH_ID = '20250103'"
    )[0][0]
    assert kept == 51


def test_rerun_keeps_counts_and_rejects(warehouse):
    run_feed(warehouse, "item_master")
    run_feed(warehouse, "store_master")
    run_feed(warehouse, "pos_sales")
    first = (
        warehouse.query("SELECT COUNT(*) FROM TMP_POS_SALES")[0][0],
        warehouse.query("SELECT COUNT(*) FROM C_REJ_TABLE")[0][0],
    )
    run_feed(warehouse, "item_master")
    run_feed(warehouse, "store_master")
    run_feed(warehouse, "pos_sales")
    second = (
        warehouse.query("SELECT COUNT(*) FROM TMP_POS_SALES")[0][0],
        warehouse.query("SELECT COUNT(*) FROM C_REJ_TABLE")[0][0],
    )
    assert first == second == (51, 5)


def test_empty_pos_file_is_fatal_and_recorded(warehouse, tmp_path):
    empty = tmp_path / "pos_empty.csv"
    empty.write_text(
        "transaction_id,line_number,item_id,store_id,sale_qty,sale_amount\n"
    )
    spec = SPECS["pos_sales"]
    bad = FeedContext("20250103", dt.date(2025, 1, 3), str(empty), spec)
    with pytest.raises(GateFileError):
        BronzePipeline(warehouse, spec, TABLES).run(bad)
    rows = warehouse.query(
        "SELECT GATE_ID, REASON_CD FROM C_REJ_TABLE WHERE SRC_TABLE_NM = 'STG_POS_SALES'"
    )
    assert rows == [("Q3", "ZERO_ROWS")]


def test_empty_inventory_delta_is_allowed(warehouse, tmp_path):
    empty = tmp_path / "inv_empty.csv"
    empty.write_text("item_id,store_id,on_hand_qty,inventory_value_amt,received_qty\n")
    spec = SPECS["inventory"]
    delta = FeedContext("20250103", dt.date(2025, 1, 3), str(empty), spec)
    assert BronzePipeline(warehouse, spec, TABLES).run(delta).rows_out == 0


def dim_check_rows(warehouse):
    from jinja2 import Template

    tables = {
        "control": {"rej_table": "C_REJ_TABLE", "alert_table": "C_ALERT_TABLE"},
        "stg": {
            "item_master": "STG_ITEM_MASTER",
            "store_master": "STG_STORE_MASTER",
            "pos_sales": "STG_POS_SALES",
            "inventory": "STG_INVENTORY",
            "price": "STG_PRICE",
        },
        "tmp": {
            "item_master": "TMP_ITEM_MASTER",
            "store_master": "TMP_STORE_MASTER",
            "pos_sales": "TMP_POS_SALES",
            "inventory": "TMP_INVENTORY",
            "price": "TMP_PRICE",
        },
        "dim": {"day": "DIM_DAY", "item": "DIM_ITEM", "locn": "DIM_LOCN"},
        "fct": {
            "sales": "FCT_SALES",
            "price": "FCT_PRICE",
            "inventory": "FCT_INVENTORY",
        },
    }
    from jinja2 import ChainableUndefined

    text = Template(
        (SQL / PIPELINE["reconcile"]).read_text(), undefined=ChainableUndefined
    ).render(tables=tables)
    segments = [
        part.strip()
        for part in text.split("UNION ALL")
        if "SELECT 'dim_" in part and "AS CHECK_NM" in part
    ]
    assert segments, "no dim segments matched"
    query = " UNION ALL ".join(segments)
    translated = [
        statement
        for statement in sqlglot.transpile(query, read="snowflake", write="duckdb")
        if statement.strip()
    ]
    assert translated, "dim query transpiled to nothing"
    return warehouse.query(translated[0])


def test_dimension_integrity_checks_pass_on_clean_versions(warehouse):
    rows = dim_check_rows(warehouse)
    assert [row[0] for row in rows] == [
        "dim_window_overlap",
        "dim_window_bounds",
        "dim_current_versions",
        "dim_window_gaps",
    ]
    assert all(int(row[2]) == 0 for row in rows)


def test_dimension_integrity_checks_catch_overlap_and_double_current(warehouse):
    warehouse.execute(
        "INSERT INTO DIM_ITEM (ITEM_KY, ITEM_ID, EFF_FROM_DT, EFF_TO_DT, CURR_FLG) "
        "VALUES (999, 1, DATE '2025-01-02', DATE '9999-12-31', TRUE)"
    )
    violations = {row[0]: int(row[2]) for row in dim_check_rows(warehouse)}
    assert violations["dim_window_overlap"] == 1
    assert violations["dim_current_versions"] == 1
    assert violations["dim_window_bounds"] == 0


def test_all_malformed_rows_fail_q3(warehouse, tmp_path):
    path = tmp_path / "pos_malformed.csv"
    path.write_text(
        "transaction_id,line_number,item_id,store_id,sale_qty,sale_amount\n1,1\n2,1,3\n"
    )
    spec = SPECS["pos_sales"]
    bad = FeedContext("20250103", dt.date(2025, 1, 3), str(path), spec)
    with pytest.raises(GateFileError):
        BronzePipeline(warehouse, spec, TABLES).run(bad)
    rows = warehouse.query(
        "SELECT GATE_ID, REASON_CD FROM C_REJ_TABLE "
        "WHERE SRC_TABLE_NM = 'STG_POS_SALES' ORDER BY GATE_ID, REASON_CD"
    )
    assert rows == [
        ("Q2", "MALFORMED_ROW"),
        ("Q2", "MALFORMED_ROW"),
        ("Q3", "ZERO_ROWS"),
    ]
    assert warehouse.query("SELECT COUNT(*) FROM STG_POS_SALES")[0][0] == 0


def test_bronze_carries_source_row_number_and_ingest_ts(warehouse):
    BronzePipeline(warehouse, SPECS["pos_sales"], TABLES).run(ctx("pos_sales"))
    rows = warehouse.query(
        "SELECT SRC_ROW_NUM, INGEST_TS, SRC_FILE_NM FROM STG_POS_SALES "
        "ORDER BY SRC_ROW_NUM"
    )
    assert [r[0] for r in rows] == list(range(1, 57))
    assert all(r[1] is not None for r in rows)
    assert rows[0][2] == "pos_sales_20250103.csv"
