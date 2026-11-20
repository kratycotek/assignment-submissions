from medallion.reconcile import check_ok, partition


def test_equality_compares_numbers_and_dates():
    assert check_ok("EQ", "10.0000", 10)
    assert check_ok("EQ", "2025-01-03", "2025-01-03")
    assert not check_ok("EQ", "2025-01-02", "2025-01-03")


def test_less_or_equal_is_a_threshold():
    assert check_ok("LE", 0, 0)
    assert check_ok("LE", 3, 5)
    assert not check_ok("LE", 6, 5)


def test_partition_splits_failures_into_hard_and_threshold():
    rows = [
        ("rows", "EQ", 5, 5),
        ("amount", "EQ", 10, 9),
        ("unknown", "LE", 3, 0),
        ("dups", "EQ", 1, 0),
    ]
    hard, soft = partition(rows)
    assert [row[0] for row in hard] == ["amount", "dups"]
    assert [row[0] for row in soft] == ["unknown"]
