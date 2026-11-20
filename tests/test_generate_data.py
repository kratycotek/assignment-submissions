import csv
from pathlib import Path

from helpers.generators import generate_data as gd


def read(path: Path):
    with path.open(newline="") as f:
        return list(csv.reader(f))


def test_generation_is_deterministic(tmp_path):
    gd.generate_into(tmp_path / "a")
    gd.generate_into(tmp_path / "b")
    for path in sorted((tmp_path / "a").iterdir()):
        assert path.read_bytes() == (tmp_path / "b" / path.name).read_bytes()


def test_five_days_of_five_files(tmp_path):
    gd.generate_into(tmp_path)
    assert len(list(tmp_path.glob("*.csv"))) == 25


def test_day_three_carries_the_five_defects(tmp_path):
    gd.generate_into(tmp_path)
    pos = read(tmp_path / "pos_sales_20250103.csv")[1:]
    items = {r[0] for r in read(tmp_path / "item_master_20250103.csv")[1:]}
    null_item = [r for r in pos if r[2] == ""]
    negative = [r for r in pos if int(r[4]) < 0]
    unknown = [r for r in pos if r[2] and r[2] not in items]
    keys = [(r[0], r[1]) for r in pos]
    duplicates = len(keys) - len(set(keys))
    assert len(null_item) == 2
    assert len(negative) == 1
    assert len(unknown) == 1
    assert duplicates >= 1
    stores = {r[0]: r[3] for r in read(tmp_path / "store_master_20250103.csv")[1:]}
    assert stores["3"] == "9"


def test_price_file_has_one_row_per_item_and_store(tmp_path):
    gd.generate_into(tmp_path)
    rows = read(tmp_path / "price_master_20250101.csv")[1:]
    keys = [(r[0], r[1]) for r in rows]
    assert len(keys) == len(set(keys)) == gd.ITEM_COUNT * gd.STORE_COUNT
