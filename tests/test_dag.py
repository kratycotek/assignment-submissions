def test_dag_parses(dag_bag):
    assert dag_bag.import_errors == {}
    assert set(dag_bag.dags) == {"medallion"}


def test_dag_has_no_cycles(dag):
    assert len(list(dag.topological_sort())) == len(dag.tasks)


def test_every_task_has_retries(dag):
    assert all(task.retries >= 1 for task in dag.tasks)


def test_daily_serial_without_catchup(dag):
    assert str(dag.schedule) == "@daily"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_stage_order(dag):
    ordered = [task.task_id for task in dag.topological_sort()]
    expected = [
        "wait_pos_sales",
        "bootstrap",
        "ingest",
        "silver_master",
        "run_dimension",
        "silver_fact",
        "run_fact",
        "reconcile",
    ]
    positions = [ordered.index(task_id) for task_id in expected]
    assert positions == sorted(positions)


def test_one_sensor_per_feed_and_mapped_work(dag, manifest):
    sensors = {task.task_id for task in dag.tasks if task.task_id.startswith("wait_")}
    assert sensors == {f"wait_{feed['name']}" for feed in manifest["feeds"]}
    mapped = {task.task_id for task in dag.tasks if task.is_mapped}
    assert mapped == {
        "ingest",
        "silver_master",
        "run_dimension",
        "silver_fact",
        "run_fact",
    }
