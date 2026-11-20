from collections.abc import Sequence

from ..warehouse import Warehouse
from .base import (
    BatchGate,
    BatchOutcome,
    FeedContext,
    GateAbortError,
    Layer,
    Record,
    ReferentialRule,
    SourceStage,
    make_reject,
)
from .tables import Tables


class SqlBatchGate(BatchGate):
    def __init__(self, warehouse: Warehouse, tables: Tables):
        self.warehouse = warehouse
        self.tables = tables


class ReferentialGate(SqlBatchGate):
    description = "A foreign key value exists in its parent table for this batch"

    def __init__(self, warehouse: Warehouse, rule: ReferentialRule, tables: Tables):
        super().__init__(warehouse, tables)
        self.code = rule.gate_code
        self._rule = rule

    def apply(self, records: Sequence[Record], ctx: FeedContext) -> BatchOutcome:
        sql = (
            f"SELECT DISTINCT {self._rule.parent_column} "
            f"FROM {self._rule.parent_table} "
            f"WHERE BATCH_ID = {self.warehouse.param}"
        )
        allowed = {row[0] for row in self.warehouse.query(sql, (ctx.batch_id,))}
        kept, rejects = [], []
        for record in records:
            value = record.data.get(self._rule.column)
            if value not in allowed:
                rejects.append(
                    make_reject(
                        ctx,
                        Layer.SILVER,
                        SourceStage.BRONZE,
                        self.code,
                        self._rule.reason_cd,
                        record,
                    )
                )
                continue
            kept.append(record)
        return BatchOutcome(kept, rejects)


class SurrogateKeyGate(SqlBatchGate):
    code = "Q10"
    description = "Dimension versions exist for the business date and provide keys"

    def __init__(self, warehouse: Warehouse, tables: Tables):
        super().__init__(warehouse, tables)

    def apply(self, records: Sequence[Record], ctx: FeedContext) -> BatchOutcome:
        p = self.warehouse.param
        day_rows = self.warehouse.query(
            f"SELECT DAY_KY FROM {self.tables.dim_day} WHERE CALENDAR_DT = {p}",
            (ctx.business_dt,),
        )
        if not day_rows:
            reject = make_reject(
                ctx,
                Layer.GOLD,
                SourceStage.SILVER,
                self.code,
                "NO_KEY_DAY",
                payload={"business_dt": ctx.business_dt.isoformat()},
            )
            return BatchOutcome([], [reject], GateAbortError(self.code, "NO_KEY_DAY"))

        items, duplicate_items = self._key_map(
            f"SELECT ITEM_ID, ITEM_KY FROM {self.tables.dim_item} "
            f"WHERE {p} >= EFF_FROM_DT AND {p} < EFF_TO_DT "
            f"ORDER BY ITEM_ID, EFF_FROM_DT",
            (ctx.business_dt, ctx.business_dt),
        )
        locns, duplicate_locns = self._key_map(
            f"SELECT LOCN_ID, LOCN_KY FROM {self.tables.dim_locn} "
            f"WHERE {p} >= EFF_FROM_DT AND {p} < EFF_TO_DT "
            f"ORDER BY LOCN_ID, EFF_FROM_DT",
            (ctx.business_dt, ctx.business_dt),
        )

        kept, rejects = [], []
        for record in records:
            item_id = record.data.get("item_id")
            store_id = record.data.get("store_id")
            if item_id in duplicate_items:
                rejects.append(self._reject(ctx, record, "AMBIGUOUS_ITEM"))
                continue
            if store_id in duplicate_locns:
                rejects.append(self._reject(ctx, record, "AMBIGUOUS_LOCN"))
                continue
            item_ky = items.get(item_id)
            locn_ky = locns.get(store_id)
            if item_ky is None:
                rejects.append(self._reject(ctx, record, "NO_KEY_ITEM"))
                continue
            if locn_ky is None:
                rejects.append(self._reject(ctx, record, "NO_KEY_LOCN"))
                continue
            record.data["ITEM_KY"] = item_ky
            record.data["LOCN_KY"] = locn_ky
            record.data["DAY_KY"] = day_rows[0][0]
            kept.append(record)
        return BatchOutcome(kept, rejects)

    def _key_map(self, sql: str, params: Sequence) -> tuple[dict, set]:
        keys: dict = {}
        duplicates: set = set()
        for business_id, surrogate_key in self.warehouse.query(sql, params):
            if business_id in keys:
                duplicates.add(business_id)
            else:
                keys[business_id] = surrogate_key
        return keys, duplicates

    def _reject(self, ctx: FeedContext, record: Record, reason_cd: str):
        return make_reject(
            ctx, Layer.GOLD, SourceStage.SILVER, self.code, reason_cd, record
        )
