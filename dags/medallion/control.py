import datetime as dt

from .gates.tables import Tables, load_tables
from .warehouse import Warehouse

CONTROL_BATCH = "__CONTROL__"
TABLES = load_tables(
    __import__("pathlib").Path(__file__).resolve().parents[2]
    / "include"
    / "sql"
    / "tables.yaml"
)


def tables(value: Tables) -> None:
    global TABLES
    TABLES = value


RELOAD_FROM = "RELOAD_FROM_DT"
RELOAD_THROUGH = "RELOAD_THROUGH_DT"
RELOAD_NEXT = "RELOAD_NEXT_DT"


class ReloadPolicyError(Exception):
    pass


def record_load(
    warehouse: Warehouse, target: str, business_dt: dt.date, batch_id: str
) -> None:
    p = warehouse.param
    warehouse.execute(
        f"MERGE INTO {TABLES.control_load_dates} AS t "
        f"USING (SELECT {p} AS TARGET_TBL_NM, {p} AS LAST_LOAD_DT) AS s "
        "ON t.TARGET_TBL_NM = s.TARGET_TBL_NM "
        "WHEN MATCHED AND t.LAST_LOAD_DT < s.LAST_LOAD_DT THEN UPDATE SET "
        f"LAST_LOAD_DT = s.LAST_LOAD_DT, BATCH_ID = {p}, REC_UPDT_TS = CURRENT_TIMESTAMP "
        "WHEN NOT MATCHED THEN INSERT (TARGET_TBL_NM, LAST_LOAD_DT, BATCH_ID) "
        f"VALUES (s.TARGET_TBL_NM, s.LAST_LOAD_DT, {p})",
        (target, business_dt, batch_id, batch_id),
    )


def watermark(warehouse: Warehouse) -> dt.date | None:
    rows = warehouse.query(f"SELECT MAX(LAST_LOAD_DT) FROM {TABLES.control_load_dates}")
    return rows[0][0] if rows else None


def write_run_params(
    warehouse: Warehouse,
    business_dt: dt.date,
    batch_id: str,
    values: dict[str, str],
) -> int:
    p = warehouse.param
    warehouse.execute(
        f"DELETE FROM {TABLES.control_param} WHERE BATCH_ID = {p}", (batch_id,)
    )
    rows = [(batch_id, name, value, business_dt) for name, value in values.items()]
    warehouse.executemany(
        f"INSERT INTO {TABLES.control_param} (BATCH_ID, PARAM_NM, PARAM_VAL, BUSINESS_DT) "
        f"VALUES ({p}, {p}, {p}, {p})",
        rows,
    )
    return len(rows)


def get_param(warehouse: Warehouse, name: str) -> str | None:
    p = warehouse.param
    rows = warehouse.query(
        f"SELECT PARAM_VAL FROM {TABLES.control_param} WHERE BATCH_ID = {p} AND PARAM_NM = {p}",
        (CONTROL_BATCH, name),
    )
    return rows[0][0] if rows else None


def set_param(warehouse: Warehouse, name: str, value: str) -> None:
    p = warehouse.param
    warehouse.execute(
        f"MERGE INTO {TABLES.control_param} AS t "
        f"USING (SELECT {p} AS BATCH_ID, {p} AS PARAM_NM, {p} AS PARAM_VAL) AS s "
        "ON t.BATCH_ID = s.BATCH_ID AND t.PARAM_NM = s.PARAM_NM "
        "WHEN MATCHED THEN UPDATE SET PARAM_VAL = s.PARAM_VAL "
        "WHEN NOT MATCHED THEN INSERT (BATCH_ID, PARAM_NM, PARAM_VAL) "
        "VALUES (s.BATCH_ID, s.PARAM_NM, s.PARAM_VAL)",
        (CONTROL_BATCH, name, value),
    )


def clear_params(warehouse: Warehouse, *names: str) -> None:
    p = warehouse.param
    for name in names:
        warehouse.execute(
            f"DELETE FROM {TABLES.control_param} WHERE BATCH_ID = {p} AND PARAM_NM = {p}",
            (CONTROL_BATCH, name),
        )


def apply_reload_policy(
    warehouse: Warehouse, business_dt: dt.date, batch_id: str
) -> None:
    next_dt = get_param(warehouse, RELOAD_NEXT)
    if next_dt is not None:
        expected = dt.date.fromisoformat(next_dt)
        if business_dt not in (expected, expected - dt.timedelta(days=1)):
            raise ReloadPolicyError(
                f"reload in progress: next required date is {next_dt}, got {business_dt}"
            )
        return

    loaded_to = watermark(warehouse)
    if loaded_to is None:
        return
    if business_dt > loaded_to and business_dt > loaded_to + dt.timedelta(days=1):
        raise ReloadPolicyError(
            f"gap detected: last loaded date is {loaded_to}, got {business_dt}"
        )
    if business_dt >= loaded_to:
        return

    set_param(warehouse, RELOAD_FROM, business_dt.isoformat())
    set_param(warehouse, RELOAD_THROUGH, loaded_to.isoformat())
    set_param(warehouse, RELOAD_NEXT, (business_dt + dt.timedelta(days=1)).isoformat())


def advance_reload(warehouse: Warehouse, business_dt: dt.date) -> bool:
    next_dt = get_param(warehouse, RELOAD_NEXT)
    if next_dt is None:
        return False
    through = dt.date.fromisoformat(get_param(warehouse, RELOAD_THROUGH))
    if business_dt < through:
        set_param(
            warehouse, RELOAD_NEXT, (business_dt + dt.timedelta(days=1)).isoformat()
        )
        return False
    clear_params(warehouse, RELOAD_FROM, RELOAD_THROUGH, RELOAD_NEXT)
    return True
