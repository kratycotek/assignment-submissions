import abc
import datetime as dt
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Layer(str, Enum):
    BRONZE = "BRONZE"
    SILVER = "SILVER"
    GOLD = "GOLD"


class SourceStage(str, Enum):
    FILE = "FILE"
    BRONZE = "BRONZE"
    SILVER = "SILVER"


class Remediation(str, Enum):
    REJECT = "REJECT"
    TRANSFORM = "TRANSFORM"


class GateAction(str, Enum):
    FAIL = "FAIL"
    QUARANTINE = "QUARANTINE"
    ALERT = "ALERT"


class GateError(Exception):
    def __init__(
        self,
        gate_code: str,
        reason_cd: str,
        column: str | None = None,
        value: Any = None,
        fatal: bool = False,
    ):
        super().__init__(f"{gate_code}: {reason_cd}")
        self.gate_code = gate_code
        self.reason_cd = reason_cd
        self.column = column
        self.value = value
        self.fatal = fatal


class GateFileError(GateError):
    def __init__(self, gate_code, reason_cd, column=None, value=None):
        super().__init__(gate_code, reason_cd, column, value, fatal=True)


class GateRowError(GateError):
    def __init__(self, gate_code, reason_cd, column=None, value=None):
        super().__init__(gate_code, reason_cd, column, value, fatal=False)


class GateAbortError(GateError):
    def __init__(self, gate_code, reason_cd, column=None, value=None):
        super().__init__(gate_code, reason_cd, column, value, fatal=True)


class GateConfigurationError(Exception):
    pass


@dataclass
class Record:
    raw: dict[str, str]
    data: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Reject:
    failed_layer: Layer
    source_stage: SourceStage
    gate_code: str
    reason_cd: str
    src_table: str
    batch_id: str
    src_file: str | None
    payload: dict[str, Any]


@dataclass
class BatchOutcome:
    kept: list[Record]
    rejects: list[Reject]
    fatal: GateError | None = None


@dataclass(frozen=True)
class PipelineResult:
    rows_in: int
    rows_out: int
    rows_rejected: int


@dataclass
class FeedOutcome:
    rows: list[Record]
    rejects: list[Reject]
    alerts: list[Reject] = field(default_factory=list)
    fatal: GateError | None = None


@dataclass(frozen=True)
class DomainRule:
    column: str
    minimum: Any
    reason_cd: str
    inclusive: bool = False


@dataclass(frozen=True)
class ReferentialRule:
    column: str
    parent_table: str
    parent_column: str
    gate_code: str
    reason_cd: str


@dataclass(frozen=True)
class FeedSpec:
    name: str
    file: str
    stg_table: str
    tmp_table: str
    columns: tuple[str, ...]
    types: dict[str, str]
    keys: tuple[str, ...]
    dedupe: tuple[str, ...]
    dedupe_reason: str
    min_rows: int
    domain: tuple[DomainRule, ...]
    referential: tuple[ReferentialRule, ...]
    lookup_keys: bool

    @classmethod
    def from_dict(cls, raw: dict) -> "FeedSpec":
        return cls(
            name=raw["name"],
            file=raw["file"],
            stg_table=raw["stg_table"],
            tmp_table=raw["tmp_table"],
            columns=tuple(raw["columns"]),
            types=dict(raw.get("types", {})),
            keys=tuple(raw.get("keys", ())),
            dedupe=tuple(raw.get("dedupe", ())),
            dedupe_reason=raw.get("dedupe_reason", "DUP_KEY"),
            min_rows=int(raw.get("min_rows", 1)),
            domain=tuple(DomainRule(**d) for d in raw.get("domain", ())),
            referential=tuple(ReferentialRule(**r) for r in raw.get("referential", ())),
            lookup_keys=bool(raw.get("lookup_keys", False)),
        )


@dataclass
class FeedContext:
    batch_id: str
    business_dt: dt.date
    path: str | None
    spec: FeedSpec


class Gate(abc.ABC):
    code: str
    description: str
    fatal: bool = False
    action: GateAction = GateAction.QUARANTINE


class FileGate(Gate):
    @abc.abstractmethod
    def check_file(self, ctx: FeedContext) -> None: ...


class RowGate(Gate):
    @abc.abstractmethod
    def check_row(self, record: Record) -> None: ...


class BatchGate(Gate):
    @abc.abstractmethod
    def apply(self, records: Sequence[Record], ctx: FeedContext) -> BatchOutcome: ...


class RawBatchGate(Gate):
    @abc.abstractmethod
    def split(
        self, rows: Sequence[Sequence[str]], ctx: FeedContext
    ) -> tuple[list[list[str]], list[Reject]]: ...


def make_reject(
    ctx: FeedContext,
    layer: Layer,
    stage: SourceStage,
    gate_code: str,
    reason_cd: str,
    record: Record | None = None,
    payload: dict[str, Any] | None = None,
) -> Reject:
    src_table = ctx.spec.tmp_table if layer is Layer.GOLD else ctx.spec.stg_table
    return Reject(
        failed_layer=layer,
        source_stage=stage,
        gate_code=gate_code,
        reason_cd=reason_cd,
        src_table=src_table,
        batch_id=ctx.batch_id,
        src_file=(record.raw.get("SRC_FILE_NM") if record else None),
        payload=payload if payload is not None else (record.raw if record else {}),
    )
