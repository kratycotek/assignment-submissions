import datetime as dt
from pathlib import Path

import pytest

from medallion.gates import (
    DedupeGate,
    DomainGate,
    DomainRule,
    FeedContext,
    FeedSpec,
    GateAction,
    GateConfigurationError,
    GateFileError,
    GateRowError,
    GateRunner,
    HeaderGate,
    NullKeyGate,
    Record,
    ReferentialGate,
    ReferentialRule,
    RowCountGate,
    RowWidthGate,
    SurrogateKeyGate,
    TypeCastGate,
    load_tables,
    parse_actions,
)

SPEC = FeedSpec(
    name="pos_sales",
    file="pos_{ds}.csv",
    stg_table="STG_POS_SALES",
    tmp_table="TMP_POS_SALES",
    columns=("transaction_id", "line_number", "item_id", "sale_qty"),
    types={
        "transaction_id": "int",
        "line_number": "int",
        "item_id": "int",
        "sale_qty": "int",
    },
    keys=("transaction_id", "line_number", "item_id"),
    dedupe=("transaction_id", "line_number"),
    dedupe_reason="DUP_LINE",
    min_rows=1,
    domain=(DomainRule("sale_qty", 0, "NEG_QTY"),),
    referential=(),
    lookup_keys=False,
)
REPO = Path(__file__).resolve().parents[1]
TABLES = load_tables(REPO / "include" / "sql" / "tables.yaml")
TYPES = {
    "transaction_id": "int",
    "line_number": "int",
    "item_id": "int",
    "store_id": "int",
    "sale_qty": "int",
}


def ctx(path=None):
    return FeedContext("b1", dt.date(2025, 1, 3), str(path) if path else None, SPEC)


def record(**raw):
    return Record(raw=raw, data={})


def typed(**raw):
    row = record(**raw)
    TypeCastGate({key: TYPES[key] for key in raw}).check_row(row)
    return row


def csv_file(tmp_path, text):
    path = tmp_path / "feed.csv"
    path.write_text(text)
    return path


@pytest.mark.parametrize(
    ("body", "valid"),
    [("a,b\n1,2\n", True), ("a,c\n1,2\n", False), ("", False)],
)
def test_header_gate(tmp_path, body, valid):
    path = csv_file(tmp_path, body)
    gate = HeaderGate(("a", "b"))
    if valid:
        gate.check_file(ctx(path))
    else:
        with pytest.raises(GateFileError) as error:
            gate.check_file(ctx(path))
        assert error.value.fatal and error.value.reason_cd == "FILE_SCHEMA"


def test_row_count_gate_counts_rows_that_survive_width():
    rows = [["1", "2"]]
    assert RowCountGate(1).split(rows, ctx())[0] == rows
    assert RowCountGate(0).split([], ctx())[0] == []
    with pytest.raises(GateFileError) as error:
        RowCountGate(1).split([], ctx())
    assert error.value.reason_cd == "ZERO_ROWS"


def test_row_width_gate_rejects_malformed_rows(tmp_path):
    kept, rejects = RowWidthGate().split(
        [["1", "2", "3", "4"], ["1", "2"]], ctx(tmp_path)
    )
    assert kept == [["1", "2", "3", "4"]]
    assert rejects[0].gate_code == "Q2"
    assert rejects[0].reason_cd == "MALFORMED_ROW"
    assert rejects[0].payload == {"fields": ["1", "2"]}


def test_type_cast_gate_casts_blanks_and_rejects_bad_values():
    row = typed(transaction_id="7", sale_qty="")
    assert row.data["transaction_id"] == 7
    assert row.data["sale_qty"] is None

    bad = record(transaction_id="x")
    with pytest.raises(GateRowError) as error:
        TypeCastGate({"transaction_id": "int"}).check_row(bad)
    assert (error.value.reason_cd, error.value.column) == (
        "TYPE_PARSE",
        "transaction_id",
    )


def test_null_key_gate_rejects_blank_key():
    row = typed(item_id="")
    with pytest.raises(GateRowError) as error:
        NullKeyGate(SPEC.keys).check_row(row)
    assert error.value.reason_cd == "NULL_KEY"


def test_domain_gate_rejects_negative_quantity():
    row = typed(sale_qty="-2")
    with pytest.raises(GateRowError) as error:
        DomainGate(SPEC.domain).check_row(row)
    assert error.value.reason_cd == "NEG_QTY"


def test_dedupe_keeps_first_and_records_loser():
    rows = [
        typed(transaction_id="1", line_number="1"),
        typed(transaction_id="1", line_number="1"),
    ]
    outcome = DedupeGate(("transaction_id", "line_number"), "DUP_LINE").apply(
        rows, ctx()
    )
    assert len(outcome.kept) == 1
    assert [(r.gate_code, r.reason_cd) for r in outcome.rejects] == [("Q7", "DUP_LINE")]


class FakeWarehouse:
    param = "?"

    def __init__(self, results):
        self._results = results
        self._query = 0

    def query(self, sql, params=()):
        result = self._results[self._query]
        self._query += 1
        return result


def test_referential_gate_rejects_unknown_item():
    rule = ReferentialRule(
        "item_id", "TMP_ITEM_MASTER", "item_id", "Q8", "UNKNOWN_ITEM"
    )
    rows = [typed(item_id="1"), typed(item_id="999")]
    outcome = ReferentialGate(FakeWarehouse([[(1,)]]), rule, TABLES).apply(rows, ctx())
    assert [r.raw["item_id"] for r in outcome.kept] == ["1"]
    assert outcome.rejects[0].reason_cd == "UNKNOWN_ITEM"


def test_surrogate_key_gate_assigns_keys_and_rejects_missing_item():
    warehouse = FakeWarehouse([[(7,)], [(1, 11)], [(1, 21)]])
    rows = [typed(item_id="1", store_id="1"), typed(item_id="9", store_id="1")]
    outcome = SurrogateKeyGate(warehouse, TABLES).apply(rows, ctx())
    assert outcome.kept[0].data["ITEM_KY"] == 11
    assert outcome.kept[0].data["LOCN_KY"] == 21
    assert outcome.kept[0].data["DAY_KY"] == 7
    assert outcome.rejects[0].reason_cd == "NO_KEY_ITEM"


def test_surrogate_key_gate_rejects_ambiguous_item_version():
    warehouse = FakeWarehouse([[(7,)], [(1, 11), (1, 12)], [(1, 21)]])
    rows = [typed(item_id="1", store_id="1")]
    outcome = SurrogateKeyGate(warehouse, TABLES).apply(rows, ctx())
    assert outcome.kept == []
    assert [(r.gate_code, r.reason_cd) for r in outcome.rejects] == [
        ("Q10", "AMBIGUOUS_ITEM")
    ]


def test_surrogate_key_gate_treats_missing_day_as_fatal():
    warehouse = FakeWarehouse([[]])
    outcome = SurrogateKeyGate(warehouse, TABLES).apply(
        [typed(item_id="1", store_id="1")], ctx()
    )
    assert outcome.kept == []
    assert outcome.fatal is not None and outcome.fatal.reason_cd == "NO_KEY_DAY"
    assert outcome.rejects[0].reason_cd == "NO_KEY_DAY"


def test_runner_alert_keeps_row_and_records_alert():
    gates = [TypeCastGate(SPEC.types), NullKeyGate(SPEC.keys)]
    gates[1].action = GateAction.ALERT
    row = record(transaction_id="", line_number="1", item_id="1", sale_qty="1")
    outcome = GateRunner(gates, []).run([row], ctx())
    assert len(outcome.rows) == 1
    assert outcome.rejects == []
    assert [alert.gate_code for alert in outcome.alerts] == ["Q5"]


def test_runner_fail_is_fatal_first_failure_wins_and_skips_batch_gates():
    gates = [TypeCastGate(SPEC.types), NullKeyGate(SPEC.keys)]
    gates[1].action = GateAction.FAIL
    row = record(transaction_id="", line_number="1", item_id="1", sale_qty="-1")
    outcome = GateRunner(gates, [DedupeGate(SPEC.dedupe, "DUP_LINE")]).run([row], ctx())
    assert outcome.rows == []
    assert outcome.fatal.gate_code == "Q5"
    assert [(r.gate_code, r.reason_cd) for r in outcome.rejects] == [("Q5", "NULL_KEY")]


def test_parse_actions_rejects_unsafe_overrides():
    assert parse_actions({"Q5": "ALERT"}) == {"Q5": GateAction.ALERT}
    assert parse_actions({"Q4": "QUARANTINE"}) == {"Q4": GateAction.QUARANTINE}
    for invalid in (
        {"Q4": "ALERT"},
        {"Q7": "ALERT"},
        {"Q5": "FAIL"},
        {"Q99": "QUARANTINE"},
    ):
        with pytest.raises(GateConfigurationError):
            parse_actions(invalid)
