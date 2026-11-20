"""Generate source data files for the Assignment 2.3 ETL pipeline.

Run with:  uv run generate-data

Layouts follow the Retail DW model in ../Assignments/Module2.1:

    item_master          -> DIM_ITEM / DIM_CLASS
    store_master         -> DIM_LOCN / DIM_DISTRICT
    pos_sales            -> FCT_SALES
    inventory_snapshot   -> FCT_INVENTORY
    price_master         -> FCT_PRICE          (added by Option A, not in the original four)

Data is generated from a fixed seed: the same seed gives byte-identical files.
Five consecutive daily sets are written to ./data and an identical copy to
./backfill. Day 3 (DEFECT_DATE) carries the five quality defects from the spec.
"""

from __future__ import annotations

import csv
import shutil
from pathlib import Path

from helpers.generators.common import project_root, seeded_rng

SEED = 20250101
DATES = ["20250101", "20250102", "20250103", "20250104", "20250105"]
DEFECT_DATE = "20250103"
SCD_EVENT_DATE = "20250104"
PRICE_CHANGE_DATE = "20250103"

ITEM_COUNT = 10
STORE_COUNT = 5

ITEM_ADJECTIVES = [
    "Classic",
    "Everyday",
    "Premium",
    "Compact",
    "Deluxe",
    "Essential",
    "Sport",
    "Urban",
    "Heritage",
    "Eco",
]
ITEM_NOUNS = [
    "Polo Shirt",
    "Denim Jacket",
    "Running Shoe",
    "Backpack",
    "Water Bottle",
    "Desk Lamp",
    "Coffee Mug",
    "Yoga Mat",
    "Umbrella",
    "Notebook",
]
BRANDS = ["Northwind", "Acme", "BluePeak", "Kestrel", "Lumen", "Trailhead", "Vantage"]
STREET_NAMES = [
    "Main Street",
    "Market Road",
    "Lake View",
    "Station Road",
    "Park Avenue",
    "Hill Crescent",
    "River Lane",
    "Mall Road",
]
STORE_NAMES = ["Central", "Riverside", "Harbor", "Uptown", "Airport"]
CLASS_NAMES = {1: "Class 1", 2: "Class 2", 3: "Class 3"}


def item_profile(item_id: int) -> dict:
    """Stable attributes for an item, independent of the file date."""
    r = seeded_rng(SEED, "item", item_id)
    return {
        "name": f"{r.choice(ITEM_ADJECTIVES)} {ITEM_NOUNS[(item_id - 1) % len(ITEM_NOUNS)]}",
        "brand": r.choice(BRANDS),
        "class_id": r.randint(1, 3),
        "unit_cost": round(r.uniform(4.0, 120.0), 2),
    }


def store_profile(store_id: int) -> dict:
    """Stable attributes for a store, independent of the file date."""
    r = seeded_rng(SEED, "store", store_id)
    return {
        "name": f"{STORE_NAMES[(store_id - 1) % len(STORE_NAMES)]} Store",
        "address": f"{r.randint(1, 999)} {r.choice(STREET_NAMES)}",
        "district_id": r.randint(1, 3),
    }


def write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def item_master_rows(date: str) -> list[list]:
    rows = []
    for i in range(1, ITEM_COUNT + 1):
        p = item_profile(i)
        name, brand, class_id = p["name"], p["brand"], p["class_id"]
        if date >= SCD_EVENT_DATE:
            if i == 5:  # SCD2: tracked name change
                name = f"{name} (2025 Edition)"
            if i == 2:  # SCD1: brand correction
                brand = "BluePeak"
        rows.append([i, name, brand, class_id, CLASS_NAMES[class_id]])
    return rows


def store_master_rows(date: str) -> list[list]:
    rows = []
    for s in range(1, STORE_COUNT + 1):
        p = store_profile(s)
        name, address, district = p["name"], p["address"], p["district_id"]
        if date >= SCD_EVENT_DATE and s == 2:  # SCD1: cosmetic rename
            name = f"{name} (Flagship)"
        if date >= DEFECT_DATE and s == 3:  # SCD2: district reassignment
            district = 9
        rows.append([s, name, address, district, f"District {district}"])
    return rows


def pos_sales_rows(date: str) -> list[list]:
    day_index = DATES.index(date) + 1
    txn_base = day_index * 100_000
    r = seeded_rng(SEED, "pos", date)
    rows = []
    for n in range(1, r.randint(15, 25) + 1):
        txn_id = txn_base + n
        store_id = r.randint(1, STORE_COUNT)
        for line in range(1, r.randint(1, 4) + 1):
            item_id = r.randint(1, ITEM_COUNT)
            qty = r.randint(1, 5)
            unit_price = round(
                item_profile(item_id)["unit_cost"] * r.uniform(0.9, 1.25), 2
            )
            rows.append(
                [txn_id, line, item_id, store_id, qty, round(qty * unit_price, 2)]
            )

    if date == DEFECT_DATE:
        defect_base = txn_base + 900
        rows.append([defect_base + 1, 1, "", r.randint(1, STORE_COUNT), 1, 19.99])
        rows.append([defect_base + 2, 1, "", r.randint(1, STORE_COUNT), 2, 24.50])
        rows.append(
            [
                defect_base + 3,
                1,
                r.randint(1, ITEM_COUNT),
                r.randint(1, STORE_COUNT),
                -2,
                54.00,
            ]
        )
        rows.append([defect_base + 4, 1, 999, r.randint(1, STORE_COUNT), 1, 12.75])
        rows.append(list(rows[0]))  # exact duplicate line
    return rows


def inventory_rows(date: str) -> list[list]:
    r = seeded_rng(SEED, "inventory", date)
    rows = []
    for i in range(1, ITEM_COUNT + 1):
        unit_cost = item_profile(i)["unit_cost"]
        for s in range(1, STORE_COUNT + 1):
            on_hand = 0 if r.random() < 0.15 else r.randint(1, 60)
            received = r.randint(0, 20) if on_hand else 0
            rows.append([i, s, on_hand, round(on_hand * unit_cost, 2), received])
    return rows


def holding_price(item_id: int, store_id: int, date: str) -> float:
    """Holding price in force for one item at one store on one day.

    Base price is unit cost times a per-item markup, adjusted by a per-store
    factor. From PRICE_CHANGE_DATE, every fourth item is re-priced up 8%.
    """
    p = item_profile(item_id)
    markup = seeded_rng(SEED, "markup", item_id).uniform(1.6, 2.2)
    store_factor = seeded_rng(SEED, "store_factor", item_id, store_id).uniform(
        0.95, 1.05
    )
    price = p["unit_cost"] * markup * store_factor
    if date >= PRICE_CHANGE_DATE and item_id % 4 == 0:
        price *= 1.08
    return round(price, 2)


def price_master_rows(date: str) -> list[list]:
    """One holding price per item and store per day (the FCT_PRICE grain)."""
    return [
        [i, s, holding_price(i, s, date)]
        for i in range(1, ITEM_COUNT + 1)
        for s in range(1, STORE_COUNT + 1)
    ]


def generate_into(directory: Path) -> None:
    """Rebuild a data directory from scratch (idempotent)."""
    if directory.exists():
        shutil.rmtree(directory)
    directory.mkdir(parents=True)

    for date in DATES:
        write_csv(
            directory / f"item_master_{date}.csv",
            ["item_id", "item_name", "brand_name", "class_id", "class_name"],
            item_master_rows(date),
        )
        write_csv(
            directory / f"store_master_{date}.csv",
            ["store_id", "store_name", "address_txt", "district_id", "district_name"],
            store_master_rows(date),
        )
        write_csv(
            directory / f"pos_sales_{date}.csv",
            [
                "transaction_id",
                "line_number",
                "item_id",
                "store_id",
                "sale_qty",
                "sale_amount",
            ],
            pos_sales_rows(date),
        )
        write_csv(
            directory / f"inventory_snapshot_{date}.csv",
            [
                "item_id",
                "store_id",
                "on_hand_qty",
                "inventory_value_amt",
                "received_qty",
            ],
            inventory_rows(date),
        )
        write_csv(
            directory / f"price_master_{date}.csv",
            ["item_id", "store_id", "holding_price_amt"],
            price_master_rows(date),
        )


def main() -> None:
    root = project_root()
    data_dir = root / "data"
    backfill_dir = root / "backfill"
    generate_into(data_dir)
    generate_into(backfill_dir)
    print(f"Generated {len(DATES)} days in {data_dir} and {backfill_dir} (seed={SEED})")


if __name__ == "__main__":
    main()
