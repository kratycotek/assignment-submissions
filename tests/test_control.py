import datetime as dt

import pytest

try:
    import duckdb
except ImportError:  # pragma: no cover
    pytest.skip("duckdb is required", allow_module_level=True)
from medallion.control import (
    ReloadPolicyError,
    advance_reload,
    apply_reload_policy,
    get_param,
    record_load,
    watermark,
    write_run_params,
)
from medallion.warehouse import DuckDbWarehouse

JAN = [dt.date(2025, 1, day) for day in range(1, 11)]


@pytest.fixture
def warehouse():
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE C_LOAD_DATES (TARGET_TBL_NM VARCHAR PRIMARY KEY, "
        "LAST_LOAD_DT DATE, BATCH_ID VARCHAR, REC_UPDT_TS TIMESTAMP)"
    )
    con.execute(
        "CREATE TABLE C_PARAM (BATCH_ID VARCHAR, PARAM_NM VARCHAR, PARAM_VAL VARCHAR, "
        "BUSINESS_DT DATE, REC_INS_TS TIMESTAMP, PRIMARY KEY (BATCH_ID, PARAM_NM))"
    )
    yield DuckDbWarehouse(con)
    con.close()


def load_days(warehouse, days):
    for day in days:
        record_load(warehouse, "FCT_SALES", day, day.strftime("%Y%m%d"))


def test_record_load_keeps_the_highest_date(warehouse):
    load_days(warehouse, [JAN[0], JAN[2]])
    record_load(warehouse, "FCT_SALES", JAN[1], "20250102")
    assert watermark(warehouse) == JAN[2]


def test_reload_opens_window_through_watermark(warehouse):
    load_days(warehouse, JAN)
    apply_reload_policy(warehouse, JAN[3], "20250104")
    assert get_param(warehouse, "RELOAD_FROM_DT") == "2025-01-04"
    assert get_param(warehouse, "RELOAD_THROUGH_DT") == "2025-01-10"
    assert get_param(warehouse, "RELOAD_NEXT_DT") == "2025-01-05"


def test_reload_must_run_in_order(warehouse):
    load_days(warehouse, JAN)
    apply_reload_policy(warehouse, JAN[3], "20250104")
    with pytest.raises(ReloadPolicyError):
        apply_reload_policy(warehouse, JAN[5], "20250106")


def test_reload_closes_after_watermark_day(warehouse):
    load_days(warehouse, JAN)
    apply_reload_policy(warehouse, JAN[3], "20250104")
    for day in JAN[4:9]:
        apply_reload_policy(warehouse, day, day.strftime("%Y%m%d"))
        assert not advance_reload(warehouse, day)
    apply_reload_policy(warehouse, JAN[9], "20250110")
    assert advance_reload(warehouse, JAN[9])
    assert get_param(warehouse, "RELOAD_NEXT_DT") is None


def test_same_date_rerun_does_not_open_reload(warehouse):
    load_days(warehouse, JAN[:2])
    apply_reload_policy(warehouse, JAN[1], "20250102")
    assert get_param(warehouse, "RELOAD_NEXT_DT") is None


def test_gap_is_refused(warehouse):
    load_days(warehouse, JAN[:1])
    with pytest.raises(ReloadPolicyError):
        apply_reload_policy(warehouse, JAN[4], "20250105")


def test_run_params_replace_batch_rows_and_leave_reload_window(warehouse):
    load_days(warehouse, JAN[:5])
    apply_reload_policy(warehouse, JAN[3], "20250104")
    write_run_params(warehouse, JAN[3], "20250104", {"RUN_ID": "a"})
    write_run_params(warehouse, JAN[3], "20250104", {"RUN_ID": "b"})
    rows = warehouse.query(
        "SELECT PARAM_NM, PARAM_VAL FROM C_PARAM WHERE BATCH_ID = '20250104'"
    )
    assert rows == [("RUN_ID", "b")]
    assert get_param(warehouse, "RELOAD_NEXT_DT") == "2025-01-05"
