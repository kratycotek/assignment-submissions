from collections.abc import Sequence
from decimal import Decimal, InvalidOperation

from .base import (
    BatchGate,
    BatchOutcome,
    DomainRule,
    FeedContext,
    GateConfigurationError,
    GateRowError,
    Layer,
    Record,
    RowGate,
    SourceStage,
    make_reject,
)

_PARSERS = {
    "int": int,
    "decimal": Decimal,
    "text": str,
}


class TypeCastGate(RowGate):
    code = "Q4"
    description = "Every value parses to its declared type; blank values become null"

    def __init__(self, types: dict[str, str]):
        unknown = set(types.values()) - set(_PARSERS)
        if unknown:
            raise GateConfigurationError(f"unknown types: {sorted(unknown)}")
        self._types = types

    def check_row(self, record: Record) -> None:
        typed: dict = {}
        for column, kind in self._types.items():
            raw = record.raw.get(column)
            if raw is None or str(raw).strip() == "":
                typed[column] = None
                continue
            value = str(raw).strip()
            try:
                typed[column] = _PARSERS[kind](value)
            except (ValueError, TypeError, InvalidOperation):
                # record.data stays empty on failure, so Q4 is not alertable:
                # keeping the row would load NULLs and later fail at the writer.
                raise GateRowError(self.code, "TYPE_PARSE", column, raw)
        record.data.update(typed)


class NullKeyGate(RowGate):
    code = "Q5"
    description = "Business keys are present after casting"

    def __init__(self, keys: Sequence[str]):
        self._keys = tuple(keys)

    def check_row(self, record: Record) -> None:
        for key in self._keys:
            if record.data.get(key) is None:
                raise GateRowError(self.code, "NULL_KEY", key, record.raw.get(key))


class DomainGate(RowGate):
    code = "Q6"
    description = "Numeric values fall inside their allowed domain"

    def __init__(self, rules: Sequence[DomainRule]):
        self._rules = tuple(rules)

    def check_row(self, record: Record) -> None:
        for rule in self._rules:
            value = record.data.get(rule.column)
            if value is None:
                raise GateRowError(self.code, rule.reason_cd, rule.column, None)
            ok = value >= rule.minimum if rule.inclusive else value > rule.minimum
            if not ok:
                raise GateRowError(self.code, rule.reason_cd, rule.column, value)


class DedupeGate(BatchGate):
    code = "Q7"
    description = "Duplicate rows on the configured grain are removed, first row wins"

    def __init__(self, grain: Sequence[str], reason_cd: str):
        self._grain = tuple(grain)
        self._reason_cd = reason_cd

    def apply(self, records: Sequence[Record], ctx: FeedContext) -> BatchOutcome:
        seen: set[tuple] = set()
        kept, rejects = [], []
        for record in records:
            key = tuple(record.data.get(c) for c in self._grain)
            if key in seen:
                rejects.append(
                    make_reject(
                        ctx,
                        Layer.SILVER,
                        SourceStage.BRONZE,
                        self.code,
                        self._reason_cd,
                        record,
                    )
                )
                continue
            seen.add(key)
            kept.append(record)
        return BatchOutcome(kept, rejects)
