from collections.abc import Sequence
from decimal import Decimal, InvalidOperation


def to_decimal(value) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def check_ok(check_type: str, left, right) -> bool:
    left_num, right_num = to_decimal(left), to_decimal(right)
    if left_num is not None and right_num is not None:
        if check_type == "LE":
            return left_num <= right_num
        return left_num == right_num
    return str(left) == str(right) if check_type == "EQ" else False


def evaluate(rows: Sequence[tuple]) -> list[tuple]:
    return [row for row in rows if not check_ok(row[1], row[2], row[3])]


def partition(rows: Sequence[tuple]) -> tuple[list[tuple], list[tuple]]:
    failures = evaluate(rows)
    hard = [row for row in failures if row[1] == "EQ"]
    soft = [row for row in failures if row[1] == "LE"]
    return hard, soft
