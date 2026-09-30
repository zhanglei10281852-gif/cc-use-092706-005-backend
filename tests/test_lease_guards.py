from __future__ import annotations

import json
import os
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.core.errors import ConflictError
from app.database import close_connection, get_connection, init_db


TEMPLATE = {
    "code": "solver-a",
    "name": "方程求解模板",
    "algorithm": "solver-a",
    "parameter_schema": {"iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000}},
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
}

START = datetime(2026, 9, 30, 2, 0, 0, tzinfo=UTC)


@pytest.fixture()
def service_env(tmp_path: Path):
    close_connection()
    os.environ["TOWNSHIP_DATABASE_PATH"] = str(tmp_path / "lease.db")
    init_db()
    clock = FrozenClock(START)
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(TEMPLATE, "administrator")
    yield service, clock
    close_connection()


def submit(service: ComputeOperationsService, key: str = "lease-000001") -> dict:
    return service.submit({
        "template_code": "solver-a",
        "project_code": "project-a",
        "requested_by": "researcher-1",
        "parameters": {"iterations": 100},
        "priority": 50,
        "idempotency_key": key,
    })


def test_heartbeat_succeeds_at_exact_expiry_boundary(service_env):
    service, clock = service_env
    task = submit(service)
    claimed = service.claim("worker-a", ["solver-a"], 10)
    assert claimed["lease_generation"] == 1
    # 租约恰好在到期时刻（>= 边界，时钟精度为秒）仍然有效，允许续租。
    clock.advance(seconds=10)
    renewed = service.heartbeat(task["id"], "worker-a", 10)
    assert renewed["status"] == "running"
    assert renewed["lease_owner"] == "worker-a"
    assert renewed["lease_expires_at"] == "2026-09-30T02:00:20+00:00"
    assert renewed["version"] == claimed["version"] + 1
    # 再走一秒即过期，续租必须被拒绝，且不得改动任何状态。
    clock.advance(seconds=11)
    with pytest.raises(ConflictError):
        service.heartbeat(task["id"], "worker-a", 10)
    details = service.get_task(task["id"])
    assert details["status"] == "running"
    assert details["lease_expires_at"] == "2026-09-30T02:00:20+00:00"
    assert details["interventions"][-1]["action"] == "lease_conflict"


def test_expired_lease_blocks_complete_and_preserves_result_version_rule(service_env):
    service, clock = service_env
    task = submit(service)
    service.claim("worker-a", ["solver-a"], 10)
    clock.advance(seconds=11)
    with pytest.raises(ConflictError):
        service.complete(task["id"], "worker-a", {"value": 1}, {"seconds": 1})
    details = service.get_task(task["id"])
    assert details["status"] == "running", "过期持有者的提交不得终结任务"
    assert details["results"] == []
    assert details["current_result_version"] is None
    # 恢复后由新工作者接手并提交：重试次数只在 claim 时增长，结果版本仍从 1 开始。
    assert service.recover_expired()["recovered"] == [task["id"]]
    claimed_b = service.claim("worker-b", ["solver-a"], 10)
    assert claimed_b["attempt_count"] == 2
    assert claimed_b["lease_generation"] == 2
    completed = service.complete(task["id"], "worker-b", {"value": 2}, {"seconds": 2}, lease_generation=2)
    assert completed["status"] == "succeeded"
    assert completed["current_result_version"] == 1
    details = service.get_task(task["id"])
    assert [row["version"] for row in details["results"]] == [1]


def test_fencing_generation_rejects_old_epoch_owner(service_env):
    service, clock = service_env
    task = submit(service)
    service.claim("worker-a", ["solver-a"], 10)
    clock.advance(seconds=11)
    service.recover_expired()
    service.claim("worker-b", ["solver-a"], 10)
    # 旧持有者带着旧代数的迟到操作（此时新租约仍有效）必须稳定返回 409。
    with pytest.raises(ConflictError):
        service.heartbeat(task["id"], "worker-a", 10, lease_generation=1)
    with pytest.raises(ConflictError):
        service.complete(task["id"], "worker-a", {"value": 9}, {}, lease_generation=1)
    with pytest.raises(ConflictError):
        service.fail(task["id"], "worker-a", "late", "迟到失败", True, lease_generation=1)
    details = service.get_task(task["id"])
    assert details["status"] == "running"
    assert details["lease_owner"] == "worker-b"
    conflict_rows = [i for i in details["interventions"] if i["action"] == "lease_conflict"]
    assert {i["actor"] for i in conflict_rows} == {"worker-a"}
    referenced_ops = {op for i in conflict_rows for op in ("heartbeat", "complete", "fail") if f"的{op}请求" in i["reason"]}
    assert referenced_ops == {"heartbeat", "complete", "fail"}
    assert all(i["batch_key"] for i in conflict_rows)
    # 错误代数即使租约未过期也不能续租；持有当前代数则可以。
    with pytest.raises(ConflictError):
        service.heartbeat(task["id"], "worker-b", 10, lease_generation=99)
    renewed = service.heartbeat(task["id"], "worker-b", 10, lease_generation=2)
    assert renewed["lease_owner"] == "worker-b"
    assert renewed["lease_generation"] == 2


def test_recovery_is_idempotent_and_conflict_records_are_deduped(service_env):
    service, clock = service_env
    task = submit(service)
    service.claim("worker-a", ["solver-a"], 10)  # version 1 -> 2
    clock.advance(seconds=11)
    # 同一迟到工作者重复续租冲突，只记录一条干预（冲突记录按租约代数+动作去重）。
    for _ in range(3):
        with pytest.raises(ConflictError):
            service.heartbeat(task["id"], "worker-a", 10)
    first = service.recover_expired()  # version 2 -> 3
    second = service.recover_expired()
    third = service.recover_expired()
    assert first == {"recovered": [task["id"]], "exhausted": []}
    assert second == third == {"recovered": [], "exhausted": []}
    details = service.get_task(task["id"])
    assert sum(1 for i in details["interventions"] if i["action"] == "lease_recovery") == 1
    assert sum(1 for i in details["interventions"] if i["action"] == "lease_conflict") == 1
    recovery = next(i for i in details["interventions"] if i["action"] == "lease_recovery")
    assert recovery["batch_key"] == ""
    assert json.loads(recovery["before_json"])["version"] == 2
    assert details["version"] == 3  # 被拒绝的请求与重复恢复都不推进版本


def test_exhausted_attempts_mark_failed_without_extra_claim(service_env):
    service, clock = service_env
    task = submit(service)
    first = service.claim("worker-a", ["solver-a"], 10)
    assert first["attempt_count"] == 1
    clock.advance(seconds=11)
    assert service.recover_expired()["recovered"] == [task["id"]]
    second = service.claim("worker-b", ["solver-a"], 10)
    assert second["attempt_count"] == 2
    assert second["lease_generation"] == 2
    clock.advance(seconds=11)
    exhausted = service.recover_expired()
    assert exhausted == {"recovered": [], "exhausted": [task["id"]]}
    details = service.get_task(task["id"])
    assert details["status"] == "failed"
    assert details["attempt_count"] == 2  # 恢复本身不增加尝试次数
    assert service.recover_expired() == {"recovered": [], "exhausted": []}


def test_concurrent_heartbeat_and_recovery_never_double_process(service_env):
    service, clock = service_env
    task = submit(service, "race-00000001")
    service.claim("worker-a", ["solver-a"], 10)  # 租约 02:00:10 到期

    outcomes: list[str] = []
    barrier = threading.Barrier(2)

    def renew() -> None:
        # 续租线程以恰好到期的时钟（仍有效）发起，与恢复事务交错执行。
        peer = ComputeOperationsService(get_connection(), FrozenClock(START + timedelta(seconds=10)))
        barrier.wait()
        try:
            peer.heartbeat(task["id"], "worker-a", 10, lease_generation=1)
            outcomes.append("renewed")
        except ConflictError:
            outcomes.append("rejected")

    def recover() -> None:
        # 恢复扫描以刚过期一秒的时钟执行。
        peer = ComputeOperationsService(get_connection(), FrozenClock(START + timedelta(seconds=11)))
        barrier.wait()
        result = peer.recover_expired()
        outcomes.append("recovered" if result["recovered"] else "noop")

    threads = [threading.Thread(target=renew), threading.Thread(target=recover)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert set(outcomes) in (
        {"renewed", "noop"},       # 续租先落地：新到期时间晚于恢复时钟，扫描不到
        {"rejected", "recovered"},  # 恢复先落地：状态已离开 running，迟到续租被稳定拒绝
    ), outcomes

    details = service.get_task(task["id"])
    if "renewed" in outcomes:
        assert details["status"] == "running"
        assert details["lease_owner"] == "worker-a"
        assert details["lease_expires_at"] == "2026-09-30T02:00:20+00:00"
        assert details["interventions"] == []
    else:
        assert details["status"] == "queued"
        assert details["lease_owner"] == ""
        assert details["lease_expires_at"] == ""
        assert {i["action"] for i in details["interventions"]} == {"lease_conflict", "lease_recovery"}

    # 推进到所有未来时刻之后：队列里至多一个可领取实体，杜绝两个工作者并存。
    clock.advance(seconds=30)
    next_claim = service.claim("worker-c", ["solver-a"], 10)
    if details["status"] == "queued":
        assert next_claim is not None and next_claim["id"] == task["id"]
        assert next_claim["lease_generation"] == 2
        assert next_claim["attempt_count"] == 2
    else:
        assert next_claim is None
