from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass
class TaskResult:
    task_id: str
    ok: bool
    duration_s: float
    error: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class DAGTask:
    task_id: str
    fn: Callable[[], Any]
    depends_on: list[str] = field(default_factory=list)


class DAGRunError(RuntimeError):
    pass


class DAG:
    """Minimal Python DAG executor in the spirit of Airflow. Tasks run in
    dependency order with topological scheduling and simple retries. The same
    task functions can be declared as Airflow PythonOperators when deploying
    to a scheduler, so moving to Airflow is a wiring change, not a rewrite."""

    def __init__(self, dag_id: str, tasks: list[DAGTask], max_retries: int = 1) -> None:
        self.dag_id = dag_id
        if len({t.task_id for t in tasks}) != len(tasks):
            raise DAGRunError("duplicate task_id in DAG")
        self._tasks = {t.task_id: t for t in tasks}
        self._max_retries = max_retries

    def _order(self) -> list[str]:
        order: list[str] = []
        visited: set[str] = set()
        temp: set[str] = set()

        def visit(node: str) -> None:
            if node in temp:
                raise DAGRunError(f"cycle detected at {node}")
            if node in visited:
                return
            if node not in self._tasks:
                raise DAGRunError(f"unknown dependency: {node}")
            temp.add(node)
            for dep in self._tasks[node].depends_on:
                visit(dep)
            temp.remove(node)
            visited.add(node)
            order.append(node)

        for task_id in self._tasks:
            visit(task_id)
        return order

    def run(self, on_task: Callable[[TaskResult], None] | None = None) -> dict[str, TaskResult]:
        results: dict[str, TaskResult] = {}
        for task_id in self._order():
            task = self._tasks[task_id]
            for attempt in range(self._max_retries + 1):
                start = time.monotonic()
                error: str | None = None
                ok = True
                try:
                    task.fn()
                except Exception as exc:  # noqa: BLE001
                    ok = False
                    error = str(exc)
                result = TaskResult(
                    task_id=task_id,
                    ok=ok,
                    duration_s=time.monotonic() - start,
                    error=error,
                )
                if on_task:
                    on_task(result)
                if ok:
                    results[task_id] = result
                    break
                if attempt < self._max_retries:
                    time.sleep(0.2)
            else:
                raise DAGRunError(f"task {task_id} failed: {error}")
        return results
