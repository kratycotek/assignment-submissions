from ..warehouse import Warehouse
from .base import FeedSpec, GateAction, GateConfigurationError
from .runner import GateRunner
from .silver import DedupeGate, DomainGate, NullKeyGate, TypeCastGate
from .sql import ReferentialGate, SurrogateKeyGate
from .tables import Tables

ACTION_GATES = frozenset({"Q4", "Q5", "Q6"})
ALERT_GATES = frozenset({"Q5", "Q6"})


def parse_actions(raw: dict[str, str] | None) -> dict[str, GateAction]:
    actions = {}
    for code, value in (raw or {}).items():
        if code not in ACTION_GATES:
            raise GateConfigurationError(
                f"{code} cannot take an action; only {sorted(ACTION_GATES)} can"
            )
        try:
            action = GateAction(value)
        except ValueError as error:
            raise GateConfigurationError(f"unknown gate action {value!r}") from error
        if action is GateAction.ALERT and code not in ALERT_GATES:
            raise GateConfigurationError(
                f"{code} cannot be ALERT; cast failures must be quarantined"
            )
        if action is GateAction.FAIL:
            raise GateConfigurationError(f"{code} cannot be FAIL; use QUARANTINE")

        actions[code] = action
    return actions


def build_runner(
    warehouse: Warehouse,
    spec: FeedSpec,
    actions: dict[str, str] | None = None,
    tables: Tables | None = None,
) -> GateRunner:
    parsed = parse_actions(actions)
    types = {column: spec.types.get(column, "text") for column in spec.columns}
    row_gates = [TypeCastGate(types), NullKeyGate(spec.keys)]
    if spec.domain:
        row_gates.append(DomainGate(spec.domain))
    for gate in row_gates:
        if gate.code in parsed:
            gate.action = parsed[gate.code]

    batch_gates = [DedupeGate(spec.dedupe, spec.dedupe_reason)]
    for rule in spec.referential:
        batch_gates.append(ReferentialGate(warehouse, rule, tables))
    if spec.lookup_keys:
        batch_gates.append(SurrogateKeyGate(warehouse, tables))
    return GateRunner(row_gates, batch_gates)
