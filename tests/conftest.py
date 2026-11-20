import json
import os
from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag

REPO = Path(__file__).resolve().parents[1]
DAG_FILE = REPO / "dags" / "medallion" / "dag.py"
SQL_ROOT = REPO / "include" / "sql"


def pytest_configure(config):
    os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow_home")
    os.environ.setdefault("AIRFLOW_VAR_SQL_ROOT", str(SQL_ROOT))


def pytest_sessionstart(session):
    data_dir = REPO / "data"
    if not data_dir.exists():
        from helpers.generators import generate_data as gd

        gd.generate_into(data_dir)
        gd.generate_into(REPO / "backfill")


@pytest.fixture(scope="session")
def dag_bag():

    return DagBag(dag_folder=str(DAG_FILE))


@pytest.fixture(scope="session")
def dag(dag_bag):
    return dag_bag.dags["medallion"]


@pytest.fixture(scope="session")
def manifest():
    return json.loads((SQL_ROOT / "pipeline.json").read_text())
