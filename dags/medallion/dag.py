import csv
import datetime as dt
import json
import re
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pendulum
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import Variable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException
from jinja2 import Template

from medallion.control import (
    advance_reload,
    apply_reload_policy,
    record_load,
    write_run_params,
)
from medallion.control import (
    tables as bind_tables,
)
from medallion.gates import (
    AlertWriter,
    BronzePipeline,
    FeedContext,
    FeedSpec,
    GateError,
    Layer,
    Reject,
    SilverPipeline,
    SourceStage,
    build_runner,
    load_tables,
)
from medallion.reconcile import check_ok, partition
from medallion.warehouse import SnowflakeWarehouse, Warehouse

SQL_ROOT = Path(Variable.get("sql_root", default="/opt/airflow/include/sql"))
PIPELINE = json.loads((SQL_ROOT / "pipeline.json").read_text())
TABLES = load_tables(SQL_ROOT / "tables.yaml")
SPECS = {raw["name"]: FeedSpec.from_dict(raw) for raw in PIPELINE["feeds"]}
FS_CONN_ID = Variable.get("fs_conn_id", default="connection-1")
DAG_PARAMS = {
    "data_root": Variable.get("data_root", default="/opt/airflow/data"),
    "snowflake_conn_id": Variable.get(
        "snowflake_conn_id", default="snowflake-connection-1"
    ),
    "snowflake_schema": Variable.get("snowflake_schema", default="BOOTCAMP"),
}


@dataclass
class RunStats:
    rows_in: int | None = None
    rows_out: int | None = None
    rows_rejected: int | None = None

    def capture(self, result) -> None:
        self.rows_in = result.rows_in
        self.rows_out = result.rows_out
        self.rows_rejected = result.rows_rejected


def run_clock() -> tuple[dt.date, str]:
    logical = pendulum.instance(get_current_context()["logical_date"])
    return logical.date(), logical.format("YYYYMMDD")


def feed_context(name: str, business_dt: dt.date, batch_id: str) -> FeedContext:
    spec = SPECS[name]
    root = Path(get_current_context()["params"]["data_root"])
    return FeedContext(
        batch_id=batch_id,
        business_dt=business_dt,
        path=str(root / spec.file.format(ds=batch_id)),
        spec=spec,
    )


@contextmanager
def warehouse_scope() -> Iterator[Warehouse]:
    params = get_current_context()["params"]
    hook = SnowflakeHook(
        snowflake_conn_id=params["snowflake_conn_id"],
        schema=params["snowflake_schema"],
    )
    warehouse = SnowflakeWarehouse(hook.get_conn(), TABLES)
    try:
        bind_tables(TABLES)
        yield warehouse
    finally:
        warehouse.close()


def statements(name: str, **values) -> list[str]:
    values.setdefault("tables", TABLES._raw)
    text = Template((SQL_ROOT / name).read_text()).render(**values)
    return [s.strip() for s in re.split(r";[ \t]*(?:\n|$)", text) if s.strip()]


@contextmanager
def logged(
    warehouse: Warehouse,
    script: str,
    business_dt: dt.date,
    batch_id: str,
    src_table: str | None = None,
    tgt_table: str | None = None,
):
    started = pendulum.now("UTC")
    status, reason = "SUCCESS", None
    stats = RunStats()
    try:
        yield stats
    except Exception as error:
        status, reason = "FAILED", str(error)[:4000]
        raise
    finally:
        ended = pendulum.now("UTC")
        p = warehouse.param
        warehouse.execute(
            f"INSERT INTO {warehouse.tables.control_batch_log} "
            "(BATCH_ID, BUSINESS_DT, SCRIPT_NM, SRC_TBL_NM, "
            "TGT_TBL_NM, STATUS_CD, ROW_IN_CNT, ROW_OUT_CNT, ROW_REJ_CNT, START_TS, "
            "END_TS, ELAPSED_SEC, FAILURE_REASON) "
            f"VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})",
            (
                batch_id,
                business_dt,
                script,
                src_table,
                tgt_table,
                status,
                stats.rows_in,
                stats.rows_out,
                stats.rows_rejected,
                started.naive(),
                ended.naive(),
                round((ended - started).total_seconds(), 3),
                reason,
            ),
        )


def run_sql(
    warehouse: Warehouse, name: str, business_dt: dt.date, batch_id: str
) -> None:
    target = PIPELINE.get("targets", {}).get(name)
    source = PIPELINE.get("sources", {}).get(name)
    with logged(warehouse, name, business_dt, batch_id, source, target):
        for statement in statements(
            name, ds=business_dt.isoformat(), params={"batch_id": batch_id}
        ):
            warehouse.execute(statement)
        if target:
            record_load(warehouse, target, business_dt, batch_id)


def run_sql_files(names: Sequence[str], business_dt: dt.date, batch_id: str) -> None:
    with warehouse_scope() as warehouse:
        for name in names:
            run_sql(warehouse, name, business_dt, batch_id)


def run_bronze(feed: dict) -> int:
    business_dt, batch_id = run_clock()
    ctx = feed_context(feed["name"], business_dt, batch_id)
    with (
        warehouse_scope() as warehouse,
        logged(warehouse, f"ingest_{feed['name']}", business_dt, batch_id),
    ):
        try:
            return BronzePipeline(warehouse, ctx.spec, TABLES).run(ctx)
        except GateError as error:
            raise AirflowFailException(
                f"{ctx.spec.name}: {error.gate_code} {error.reason_cd}"
            ) from error


def run_silver(name: str) -> int:
    business_dt, batch_id = run_clock()
    ctx = feed_context(name, business_dt, batch_id)
    with (
        warehouse_scope() as warehouse,
        logged(warehouse, f"silver_{name}", business_dt, batch_id),
    ):
        runner = build_runner(warehouse, ctx.spec, PIPELINE.get("gate_actions"), TABLES)
        try:
            return SilverPipeline(warehouse, ctx.spec, runner, TABLES).run(ctx)
        except GateError as error:
            raise AirflowFailException(
                f"{ctx.spec.name}: {error.gate_code} {error.reason_cd}"
            ) from error


def _amount(value: str) -> Decimal:
    try:
        return Decimal(value)
    except InvalidOperation:
        return Decimal(0)


def source_metrics(root: Path, batch_id: str) -> dict:
    metrics = {}
    for feed in PIPELINE["feeds"]:
        with (root / feed["file"].format(ds=batch_id)).open(newline="") as handle:
            rows = list(csv.reader(handle))[1:]
        good = [r for r in rows if len(r) == len(feed["columns"])]
        metrics[f"{feed['name']}_rows"] = len(rows)
        if feed["name"] == "pos_sales":
            metrics["pos_amount"] = str(
                sum(_amount(r[5]) for r in good if r[5].strip()).quantize(
                    Decimal("0.0001")
                )
            )
    return metrics


def reconcile_run(business_dt: dt.date, batch_id: str) -> list[tuple]:
    root = Path(get_current_context()["params"]["data_root"])
    with warehouse_scope() as warehouse:
        query = statements(
            PIPELINE["reconcile"],
            ds=business_dt.isoformat(),
            params={"batch_id": batch_id},
            source=source_metrics(root, batch_id),
            thresholds=PIPELINE.get("thresholds", {}),
            tables=TABLES._raw,
        )[0]
        rows = warehouse.query(query)
        hard, soft = partition(rows)
        alerts = [
            Reject(
                failed_layer=Layer.GOLD,
                source_stage=SourceStage.SILVER,
                gate_code="Q11",
                reason_cd="THRESHOLD_BREACH",
                src_table="RECONCILE",
                batch_id=batch_id,
                src_file=None,
                payload={
                    "check": name,
                    "type": check_type,
                    "left": left,
                    "right": right,
                },
            )
            for name, check_type, left, right in soft
        ]
        writer = AlertWriter(warehouse, TABLES.control_alert_table)
        writer.clear(Layer.GOLD, "RECONCILE", batch_id)
        writer.write(alerts)
        if not hard:
            advance_reload(warehouse, business_dt)
    return rows


def create_file_sensor(feed: dict) -> FileSensor:
    return FileSensor(
        task_id=f"wait_{feed['name']}",
        filepath="{{ params.data_root }}/"
        + feed["file"].replace("{ds}", "{{ ds_nodash }}"),
        fs_conn_id=FS_CONN_ID,
        poke_interval=30,
        timeout=600,
        mode="reschedule",
    )


@dag(
    "medallion",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=2)},
    tags=["snowflake", "medallion", "retail"],
    params=DAG_PARAMS,
)
def medallion():

    @task
    def bootstrap():
        business_dt, batch_id = run_clock()
        context = get_current_context()
        params = context["params"]
        with warehouse_scope() as warehouse:
            for name in PIPELINE["bootstrap"]:
                run_sql(warehouse, name, business_dt, batch_id)
            write_run_params(
                warehouse,
                business_dt,
                batch_id,
                {
                    "BUSINESS_DT": business_dt.isoformat(),
                    "RUN_ID": context.get("run_id", ""),
                    "DAG_ID": context["dag"].dag_id,
                    "SNOWFLAKE_SCHEMA": params["snowflake_schema"],
                    "SNOWFLAKE_CONN_ID": params["snowflake_conn_id"],
                    "DATA_ROOT": params["data_root"],
                    "GATE_ACTIONS": json.dumps(PIPELINE.get("gate_actions", {})),
                    "THRESHOLDS": json.dumps(PIPELINE.get("thresholds", {})),
                    "TABLE_NAMES": json.dumps(TABLES._raw),
                },
            )
            try:
                apply_reload_policy(warehouse, business_dt, batch_id)
            except Exception as error:
                raise AirflowFailException(str(error)) from error

    @task
    def ingest(feed: dict) -> int:
        return run_bronze(feed)

    @task
    def silver_master(name: str) -> int:
        return run_silver(name)

    @task
    def silver_fact(name: str) -> int:
        return run_silver(name)

    @task
    def run_dimension(name: str):
        business_dt, batch_id = run_clock()
        run_sql_files([name], business_dt, batch_id)

    @task
    def run_fact(name: str):
        business_dt, batch_id = run_clock()
        run_sql_files([name], business_dt, batch_id)

    @task(task_id="reconcile")
    def reconcile():
        business_dt, batch_id = run_clock()
        rows = reconcile_run(business_dt, batch_id)
        for name, check_type, left, right in rows:
            if check_ok(check_type, left, right):
                mark = "OK"
            else:
                mark = "MISMATCH" if check_type == "EQ" else "ALERT"
            print(f"{name}: {left} {check_type} {right} {mark}")
        hard, _ = partition(rows)
        if hard:
            raise AirflowFailException(
                f"Reconciliation failed: {[row[0] for row in hard]}"
            )

    sensors = [create_file_sensor(feed) for feed in PIPELINE["feeds"]]
    boot = bootstrap()
    loaded = ingest.expand(feed=PIPELINE["feeds"])
    masters = silver_master.expand(name=PIPELINE["silver_masters"])
    dimensions = run_dimension.expand(name=PIPELINE["gold_dimensions"])
    facts_source = silver_fact.expand(name=PIPELINE["silver_facts"])
    facts = run_fact.expand(name=PIPELINE["gold_facts"])
    checked = reconcile()

    (
        sensors
        >> boot
        >> loaded
        >> masters
        >> dimensions
        >> facts_source
        >> facts
        >> checked
    )


medallion()
