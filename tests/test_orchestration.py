from __future__ import annotations

import pytest

from eurostream.orchestration import DAG, DAGRunError, DAGTask


def test_dag_respects_dependency_order():
    order = []

    def a():
        order.append("a")

    def b():
        order.append("b")

    dag = DAG(dag_id="t", tasks=[DAGTask("b", b, depends_on=["a"]), DAGTask("a", a)])
    dag.run()
    assert order == ["a", "b"]


def test_dag_cycle_detection():
    dag = DAG(
        dag_id="t",
        tasks=[
            DAGTask("a", lambda: None, depends_on=["b"]),
            DAGTask("b", lambda: None, depends_on=["a"]),
        ],
    )
    with pytest.raises(DAGRunError, match="cycle"):
        dag.run()


def test_dag_retry_succeeds_on_second_attempt():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("first fail")

    dag = DAG(dag_id="t", tasks=[DAGTask("a", flaky)], max_retries=1)
    res = dag.run()
    assert res["a"].ok
    assert calls["n"] == 2


def test_dag_fails_after_retries_exhausted():
    dag = DAG(dag_id="t", tasks=[DAGTask("a", lambda: 1 / 0)], max_retries=1)
    with pytest.raises(DAGRunError):
        dag.run()


def test_dag_on_task_callback():
    seen = []
    dag = DAG(dag_id="t", tasks=[DAGTask("a", lambda: None)])
    dag.run(on_task=lambda r: seen.append(r.task_id))
    assert seen == ["a"]
