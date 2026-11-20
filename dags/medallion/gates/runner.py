import csv
import datetime as dt
import json
from collections.abc import Sequence

from ..warehouse import Warehouse
from .base import (
    BatchGate,
    FeedContext,
    FeedOutcome,
    GateAbortError,
    GateAction,
    GateFileError,
    GateRowError,
    Layer,
    PipelineResult,
    Record,
    Reject,
    RowGate,
    SourceStage,
    make_reject,
)
from .bronze import HeaderGate, RowCountGate, RowWidthGate
from .tables import Tables

REJECT_COLUMNS = (
    "FAILED_LAYER_CD",
    "SOURCE_STAGE",
    "GATE_ID",
    "REASON_CD",
    "SRC_TABLE_NM",
    "BATCH_ID",
    "SRC_FILE_NM",
    "PAYLOAD",
)
ALERT_COLUMNS = REJECT_COLUMNS
META_COLUMNS = ("BATCH_ID", "SRC_FILE_NM", "SRC_ROW_NUM", "INGEST_TS", "SRC_LOAD_TS")
TMP_META_COLUMNS = ("BATCH_ID", "SRC_FILE_NM", "SRC_LOAD_TS")
SURROGATE_COLUMNS = ("ITEM_KY", "LOCN_KY", "DAY_KY")


class RejectWriter:
    def __init__(self, warehouse: Warehouse, table: str):
        self._warehouse = warehouse
        self._table = table

    def clear(self, layer: Layer, src_table: str, batch_id: str) -> None:
        p = self._warehouse.param
        self._warehouse.execute(
            f"DELETE FROM {self._table} WHERE BATCH_ID = {p} "
            f"AND SRC_TABLE_NM = {p} AND FAILED_LAYER_CD = {p}",
            (batch_id, src_table, layer.value),
        )

    def write(self, rejects: Sequence[Reject]) -> int:
        if not rejects:
            return 0
        placeholders = ", ".join(
            [
                self._warehouse.json_param if c == "PAYLOAD" else self._warehouse.param
                for c in REJECT_COLUMNS
            ]
        )
        sql = (
            f"INSERT INTO {self._table} ({', '.join(REJECT_COLUMNS)}) "
            f"SELECT {placeholders}"
        )
        self._warehouse.executemany(
            sql,
            [
                (
                    r.failed_layer.value,
                    r.source_stage.value,
                    r.gate_code,
                    r.reason_cd,
                    r.src_table,
                    r.batch_id,
                    r.src_file,
                    json.dumps(r.payload, default=str),
                )
                for r in rejects
            ],
        )
        return len(rejects)


class AlertWriter:
    def __init__(self, warehouse: Warehouse, table: str):
        self._warehouse = warehouse
        self._table = table

    def clear(self, layer: Layer, src_table: str, batch_id: str) -> None:
        p = self._warehouse.param
        self._warehouse.execute(
            f"DELETE FROM {self._table} WHERE BATCH_ID = {p} "
            f"AND SRC_TABLE_NM = {p} AND FAILED_LAYER_CD = {p}",
            (batch_id, src_table, layer.value),
        )

    def write(self, alerts: Sequence[Reject]) -> int:
        if not alerts:
            return 0
        placeholders = ", ".join(
            [
                self._warehouse.json_param if c == "PAYLOAD" else self._warehouse.param
                for c in ALERT_COLUMNS
            ]
        )
        sql = (
            f"INSERT INTO {self._table} ({', '.join(ALERT_COLUMNS)}) "
            f"SELECT {placeholders}"
        )
        self._warehouse.executemany(
            sql,
            [
                (
                    a.failed_layer.value,
                    a.source_stage.value,
                    a.gate_code,
                    a.reason_cd,
                    a.src_table,
                    a.batch_id,
                    a.src_file,
                    json.dumps(a.payload, default=str),
                )
                for a in alerts
            ],
        )
        return len(alerts)


class SilverWriter:
    def __init__(self, warehouse: Warehouse, tmp_table: str, columns: Sequence[str]):
        self._warehouse = warehouse
        self._tmp_table = tmp_table
        self._columns = tuple(columns)

    def replace(self, batch_id: str, rows: Sequence[dict]) -> int:
        p = self._warehouse.param
        self._warehouse.execute(
            f"DELETE FROM {self._tmp_table} WHERE BATCH_ID = {p}", (batch_id,)
        )
        return self._warehouse.insert_rows(
            self._tmp_table,
            self._columns,
            [tuple(row.get(c) for c in self._columns) for row in rows],
        )


class GateRunner:
    def __init__(self, row_gates: Sequence[RowGate], batch_gates: Sequence[BatchGate]):
        self._row_gates = tuple(row_gates)
        self._batch_gates = tuple(batch_gates)

    def _action_for(self, code: str) -> GateAction:
        for gate in self._row_gates:
            if gate.code == code:
                return gate.action
        return GateAction.QUARANTINE

    def run(self, records: Sequence[Record], ctx: FeedContext) -> FeedOutcome:
        kept, rejects, alerts = [], [], []
        fatal: GateRowError | None = None
        for record in records:
            try:
                for gate in self._row_gates:
                    gate.check_row(record)
            except GateRowError as error:
                reject = make_reject(
                    ctx,
                    Layer.SILVER,
                    SourceStage.BRONZE,
                    error.gate_code,
                    error.reason_cd,
                    record,
                )
                action = self._action_for(error.gate_code)
                if action is GateAction.ALERT:
                    alerts.append(reject)
                    kept.append(record)
                elif action is GateAction.FAIL:
                    rejects.append(reject)
                    fatal = error
                    break
                else:
                    rejects.append(reject)
                continue
            kept.append(record)

        if fatal is None:
            for gate in self._batch_gates:
                outcome = gate.apply(kept, ctx)
                kept, batch_rejects = outcome.kept, outcome.rejects
                rejects.extend(batch_rejects)
                if outcome.fatal is not None:
                    fatal = outcome.fatal
                    break
        return FeedOutcome(kept, rejects, alerts, fatal)


class BronzePipeline:
    def __init__(self, warehouse: Warehouse, spec, tables: Tables):
        self._warehouse = warehouse
        self._spec = spec
        self._header = HeaderGate(spec.columns)
        self._row_count = RowCountGate(spec.min_rows)
        self._width = RowWidthGate()
        self._rejects = RejectWriter(warehouse, tables.control_rej_table)

    def _reject_file(self, ctx: FeedContext, error: GateFileError) -> None:
        self._rejects.write(
            [
                make_reject(
                    ctx,
                    Layer.BRONZE,
                    SourceStage.FILE,
                    error.gate_code,
                    error.reason_cd,
                    payload={"value": error.value},
                )
            ]
        )

    def run(self, ctx: FeedContext) -> PipelineResult:
        self._rejects.clear(Layer.BRONZE, self._spec.stg_table, ctx.batch_id)
        try:
            self._header.check_file(ctx)
        except GateFileError as error:
            self._reject_file(ctx, error)
            raise

        fields, rejects = self._width.split(self._read_fields(ctx.path), ctx)
        self._rejects.write(rejects)
        try:
            fields, _ = self._row_count.split(fields, ctx)
        except GateFileError as error:
            self._reject_file(ctx, error)
            raise

        loaded_at = dt.datetime.now(dt.UTC).replace(tzinfo=None)
        rows = [
            list(row)
            + [ctx.batch_id, _basename(ctx.path), number, loaded_at, loaded_at]
            for number, row in enumerate(fields, start=1)
        ]
        p = self._warehouse.param
        self._warehouse.execute(
            f"DELETE FROM {self._spec.stg_table} WHERE BATCH_ID = {p}",
            (ctx.batch_id,),
        )
        written = self._warehouse.insert_rows(
            self._spec.stg_table,
            tuple(self._spec.columns) + META_COLUMNS,
            rows,
        )
        return PipelineResult(
            rows_in=len(fields) + len(rejects),
            rows_out=written,
            rows_rejected=len(rejects),
        )

    @staticmethod
    def _read_fields(path: str) -> list[list[str]]:
        with open(path, newline="") as handle:
            reader = csv.reader(handle)
            next(reader, None)
            return [list(row) for row in reader]


class SilverPipeline:
    def __init__(self, warehouse: Warehouse, spec, runner: GateRunner, tables: Tables):
        self._warehouse = warehouse
        self._spec = spec
        self._runner = runner
        columns = tuple(spec.columns) + (SURROGATE_COLUMNS if spec.lookup_keys else ())
        self._writer = SilverWriter(
            warehouse, spec.tmp_table, columns + TMP_META_COLUMNS
        )
        self._rejects = RejectWriter(warehouse, tables.control_rej_table)
        self._alerts = AlertWriter(warehouse, tables.control_alert_table)

    def run(self, ctx: FeedContext) -> PipelineResult:
        self._rejects.clear(Layer.SILVER, self._spec.stg_table, ctx.batch_id)
        self._rejects.clear(Layer.GOLD, self._spec.tmp_table, ctx.batch_id)
        self._alerts.clear(Layer.SILVER, self._spec.stg_table, ctx.batch_id)
        records = self._read(ctx)
        outcome = self._runner.run(records, ctx)
        self._rejects.write(outcome.rejects)
        self._alerts.write(outcome.alerts)
        written = self._writer.replace(
            ctx.batch_id, [record.data for record in outcome.rows]
        )
        if outcome.fatal is not None:
            raise GateAbortError(
                outcome.fatal.gate_code,
                outcome.fatal.reason_cd,
                outcome.fatal.column,
                outcome.fatal.value,
            )
        return PipelineResult(
            rows_in=len(records),
            rows_out=written,
            rows_rejected=len(outcome.rejects),
        )

    def _read(self, ctx: FeedContext) -> list[Record]:
        columns = tuple(self._spec.columns) + TMP_META_COLUMNS
        p = self._warehouse.param
        order = ", ".join(self._spec.columns)
        rows = self._warehouse.query(
            f"SELECT {', '.join(columns)} FROM {self._spec.stg_table} "
            f"WHERE BATCH_ID = {p} ORDER BY {order}",
            (ctx.batch_id,),
        )
        records = []
        for row in rows:
            raw = {c: row[i] for i, c in enumerate(columns)}
            data = {c: raw[c] for c in TMP_META_COLUMNS}
            records.append(Record(raw=raw, data=data))
        return records


def _basename(path: str) -> str:
    return path.rsplit("/", 1)[-1]
