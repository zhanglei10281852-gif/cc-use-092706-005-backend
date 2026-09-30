from __future__ import annotations

import json
import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.core.errors import ConflictError
from app.database import get_connection, init_db, transaction


TEMPLATE = {
    "code": "solver-a",
    "name": "方程求解模板",
    "algorithm": "solver-a",
    "parameter_schema": {
        "iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000},
        "mode": {"type": "string", "required": True, "choices": ["fast", "accurate"]},
    },
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
}

START = datetime(2026, 9, 30, 2, 0, tzinfo=UTC)


@pytest.fixture()
def service_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("TOWNSHIP_DATABASE_PATH", str(tmp_path / "leases.db"))
    from app.database import close_connection

    close_connection()
    init_db()
    clock = FrozenClock(START)
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(TEMPLATE, "administrator")
    yield service, clock, tmp_path / "leases.db"
    close_connection()


def submit(service: ComputeOperationsService, key: str, *, max_attempts: int | None = None) -> dict:
    payload = {
        "template_code": "solver-a",
        "project_code": "project-a",
        "requested_by": "researcher-1",
        "parameters": {"iterations": 100, "mode": "accurate"},
        "priority": 50,
        "idempotency_key": key,
    }
    task = service.submit(payload)
    if max_attempts is not None:
        with transaction(immediate=True) as connection:
            connection.execute("UPDATE compute_tasks SET max_attempts=? WHERE id=?", (max_attempts, task["id"]))
    return service.get_task(task["id"])


def independent_service(frozen_at: datetime) -> ComputeOperationsService:
    # 写事务内部使用线程局部 get_connection()，因此在线程内构造即可拿到独立连接；
    # 注入时钟保证心跳视角在租期内、恢复视角已过期。
    return ComputeOperationsService(get_connection(), FrozenClock(frozen_at))


def test_renewal_succeeds_at_exact_expiry_boundary_but_recovery_does_not_fire(service_env):
    service, clock, _ = service_env
    task = submit(service, "lease-boundary-01")
    claimed = service.claim("worker-a", ["solver-a"], 10)
    assert claimed["lease_epoch"] == 1

    # 恰好在到期时刻：heartbeat 采用闭区间 lease_expires_at>=now，允许续期；
    # 恢复扫描采用严格小于，同一时刻不得接管。
    clock.advance(seconds=10)
    assert service.recover_expired() == {"recovered": [], "exhausted": []}
    renewed = service.heartbeat(task["id"], "worker-a", 10)
    assert renewed["status"] == "running"
    assert renewed["lease_owner"] == "worker-a"
    assert renewed["lease_epoch"] == 1
    assert renewed["lease_expires_at"] == (clock.current + timedelta(seconds=10)).isoformat(timespec="seconds")

    # 续期后旧到期时间被覆盖，稍后扫描仍找不到它。
    clock.advance(seconds=5)
    assert service.recover_expired() == {"recovered": [], "exhausted": []}


def test_expired_holder_cannot_renew_or_submit_before_recovery(service_env):
    service, clock, _ = service_env
    task = submit(service, "lease-expired-late-01")
    claimed = service.claim("worker-a", ["solver-a"], 10)
    epoch = claimed["lease_epoch"]
    clock.advance(seconds=11)

    # 恢复尚未运行，状态表面上还是 running，但旧持有者已经丧失写入资格。
    with pytest.raises(ConflictError) as heartbeat_exc:
        service.heartbeat(task["id"], "worker-a", 10, lease_epoch=epoch)
    assert heartbeat_exc.value.status_code == 409

    with pytest.raises(ConflictError):
        service.complete(task["id"], "worker-a", {"value": 1}, {}, lease_epoch=epoch)

    with pytest.raises(ConflictError):
        service.fail(task["id"], "worker-a", "boom", "炸了", True, lease_epoch=epoch)

    details = service.get_task(task["id"])
    assert details["status"] == "running" and details["lease_owner"] == "worker-a"
    assert details["results"] == []
    actions = [item["action"] for item in details["interventions"]]
    assert actions == ["lease_conflict", "lease_conflict", "lease_conflict"]
    conflict = details["interventions"][-1]
    assert conflict["actor"] == "worker-a"
    assert json.loads(conflict["detail_json"])["requested_action"] == "report_failure"


def test_recovery_is_idempotent_and_old_worker_gets_stable_conflict(service_env):
    service, clock, _ = service_env
    task = submit(service, "lease-recovery-idem-01")
    claimed = service.claim("worker-a", ["solver-a"], 10)
    epoch = claimed["lease_epoch"]
    clock.advance(seconds=11)

    first = service.recover_expired()
    assert first == {"recovered": [task["id"]], "exhausted": []}
    details = service.get_task(task["id"])
    assert details["status"] == "queued"
    assert details["lease_owner"] == "" and details["lease_expires_at"] == ""
    assert details["lease_epoch"] == epoch + 1
    recoveries = [i for i in details["interventions"] if i["action"] == "lease_recovery"]
    assert len(recoveries) == 1
    assert recoveries[0]["lease_epoch"] == epoch

    # 重复执行恢复：空结果、状态与代次不变、不重复留痕。
    second = service.recover_expired()
    assert second == {"recovered": [], "exhausted": []}
    details = service.get_task(task["id"])
    assert details["status"] == "queued" and details["lease_epoch"] == epoch + 1
    assert len([i for i in details["interventions"] if i["action"] == "lease_recovery"]) == 1

    # 旧工作者的迟到心跳/结果：稳定 409，并留下可追踪记录，任务不被重新接回。
    with pytest.raises(ConflictError):
        service.heartbeat(task["id"], "worker-a", 10, lease_epoch=epoch)
    with pytest.raises(ConflictError):
        service.complete(task["id"], "worker-a", {"value": 9}, {}, lease_epoch=epoch)
    details = service.get_task(task["id"])
    assert details["status"] == "queued"
    assert details["results"] == []
    assert len([i for i in details["interventions"] if i["action"] == "lease_conflict"]) == 2


def test_new_holder_completes_while_stale_holder_is_fenced(service_env):
    service, clock, _ = service_env
    task = submit(service, "lease-epoch-fence-01")
    first_claim = service.claim("worker-a", ["solver-a"], 10)
    stale_epoch = first_claim["lease_epoch"]
    clock.advance(seconds=11)
    service.recover_expired()

    # 任务重新排队，新工作者领取得到新代次。
    reclaimed = service.claim("worker-b", ["solver-a"], 10)
    assert reclaimed["attempt_count"] == 2
    current_epoch = reclaimed["lease_epoch"]
    assert current_epoch == stale_epoch + 2

    completed = service.complete(task["id"], "worker-b", {"value": 3.14}, {"seconds": 2}, lease_epoch=current_epoch)
    assert completed["status"] == "succeeded"

    # 旧工作者迟到提交：不得产生第二个结果版本。
    with pytest.raises(ConflictError):
        service.complete(task["id"], "worker-a", {"value": 2.71}, {}, lease_epoch=stale_epoch)

    details = service.get_task(task["id"])
    assert details["current_result_version"] == 1
    assert [row["version"] for row in details["results"]] == [1]
    assert details["results"][0]["created_by"] == "worker-b"


def test_heartbeat_without_epoch_still_fenced_by_owner_and_expiry(service_env):
    service, clock, _ = service_env
    task = submit(service, "lease-noepoch-01")
    service.claim("worker-a", ["solver-a"], 10)
    assert service.heartbeat(task["id"], "worker-a", 10)["lease_owner"] == "worker-a"
    with pytest.raises(ConflictError):
        service.heartbeat(task["id"], "worker-other", 10)
    clock.advance(seconds=11)
    with pytest.raises(ConflictError):
        service.heartbeat(task["id"], "worker-a", 10)


def test_exhausted_recovery_marks_failed_and_preserves_attempt_count(service_env):
    service, clock, _ = service_env
    task = submit(service, "lease-exhaust-01", max_attempts=1)
    claimed = service.claim("worker-a", ["solver-a"], 10)
    assert claimed["attempt_count"] == 1
    clock.advance(seconds=11)
    result = service.recover_expired()
    assert result == {"recovered": [], "exhausted": [task["id"]]}
    details = service.get_task(task["id"])
    assert details["status"] == "failed"
    assert details["attempt_count"] == 1
    assert details["lease_epoch"] == 2


def test_stale_recovery_snapshot_cannot_override_committed_heartbeat(service_env):
    """关键交错：心跳先提交续期后，携带旧快照（旧到期时间/旧代次）的恢复 CAS 必须落空且不留痕。"""
    service, clock, db_path = service_env
    task = submit(service, "lease-stale-snapshot-01")
    claimed = service.claim("worker-a", ["solver-a"], 10)
    epoch = claimed["lease_epoch"]
    stale_expires = (START + timedelta(seconds=10)).isoformat(timespec="seconds")

    # 工作者在租约窗口内（t0+5）先续期成功，到期时间推进到 t0+15（代次不变）。
    clock.current = START + timedelta(seconds=5)
    renewed = service.heartbeat(task["id"], "worker-a", 10, lease_epoch=epoch)
    assert renewed["lease_expires_at"] == (START + timedelta(seconds=15)).isoformat(timespec="seconds")

    # 模拟恢复扫描晚到、仍手持旧快照（expires=t0+10、epoch=1、now=t0+15）：
    # 行级 CAS 的 expires<? 与 epoch=? 均不再成立，更新落空。
    late_recovery = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    late_recovery.row_factory = sqlite3.Row
    now15 = (START + timedelta(seconds=15)).isoformat(timespec="seconds")
    cursor = late_recovery.execute(
        "UPDATE compute_tasks SET status='queued',lease_owner='',lease_expires_at='',lease_epoch=lease_epoch+1,available_at=?,last_error_code='lease_expired',last_error_message='工作者租约已过期',finished_at=NULL,updated_at=?,version=version+1 WHERE id=? AND status='running' AND lease_expires_at<>'' AND lease_expires_at<? AND lease_owner=? AND lease_epoch=?",
        (now15, now15, task["id"], now15, "worker-a", epoch),
    )
    assert cursor.rowcount == 0
    late_recovery.close()

    details = service.get_task(task["id"])
    assert details["status"] == "running"
    assert details["lease_owner"] == "worker-a"
    assert details["lease_expires_at"] != stale_expires
    assert details["lease_epoch"] == epoch
    assert details["interventions"] == []

    # 恰好在新到期点（恢复严格小于、心跳闭区间）不接管；再走一秒才接管并只留一次痕。
    clock.current = START + timedelta(seconds=15)
    assert service.recover_expired() == {"recovered": [], "exhausted": []}
    clock.current = START + timedelta(seconds=16)
    assert service.recover_expired() == {"recovered": [task["id"]], "exhausted": []}
    details = service.get_task(task["id"])
    assert len([i for i in details["interventions"] if i["action"] == "lease_recovery"]) == 1


def test_concurrent_heartbeat_and_recovery_never_double_process(service_env):
    """并发事务压测：任意串行化顺序下，每个任务只被一方处理，绝不重复执行。"""
    service, clock, _ = service_env
    iterations = 24
    tasks: list[dict] = []
    for index in range(iterations):
        task = submit(service, f"lease-race-{index:06d}")
        claimed = service.claim("worker-a", ["solver-a"], 10)
        assert claimed["id"] == task["id"]
        tasks.append(task)
        # 领取统一发生在 t0（并发线程各自注入 t0+5 / t0+15 的时钟）。
        clock.current = START

    outcomes: list[dict] = []
    outcomes_lock = threading.Lock()

    def run_one(task: dict) -> None:
        result: dict = {"task_id": task["id"], "heartbeat": None, "error": None}
        barrier = threading.Barrier(2)

        def heartbeat_side() -> None:
            # 服务在线程内构造，写事务走该线程的独立连接；时钟固定在租约窗口内。
            worker = independent_service(START + timedelta(seconds=5))
            barrier.wait()
            try:
                renewed = worker.heartbeat(task["id"], "worker-a", 10, lease_epoch=1)
                result["heartbeat"] = "renewed" if renewed["lease_owner"] == "worker-a" else "lost"
            except ConflictError:
                result["heartbeat"] = "conflict"
            except Exception as exc:  # pragma: no cover - 并发测试中的意外
                result["error"] = repr(exc)

        def recovery_side() -> None:
            recovery = independent_service(START + timedelta(seconds=15))
            barrier.wait()
            try:
                recovery.recover_expired()
            except Exception as exc:  # pragma: no cover
                result["error"] = repr(exc)

        threads = [threading.Thread(target=heartbeat_side), threading.Thread(target=recovery_side)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        with outcomes_lock:
            outcomes.append(result)

    threads = [threading.Thread(target=run_one, args=(task,)) for task in tasks]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(outcomes) == iterations
    for result in outcomes:
        assert result["error"] is None, result
        details = service.get_task(result["task_id"])
        if result["heartbeat"] == "renewed":
            # 心跳胜出：仍由 worker-a 以同一代次持有，无恢复/冲突留痕。
            assert details["status"] == "running"
            assert details["lease_owner"] == "worker-a"
            assert details["lease_epoch"] == 1
            assert not [i for i in details["interventions"] if i["action"] in {"lease_recovery", "lease_conflict"}]
        else:
            # 恢复胜出：迟到心跳收到稳定冲突，任务入队、代次推进，两种留痕各至多一条。
            assert result["heartbeat"] == "conflict", result
            assert details["status"] == "queued"
            assert details["lease_owner"] == ""
            assert details["lease_epoch"] == 2
            assert len([i for i in details["interventions"] if i["action"] == "lease_recovery"]) == 1
            assert len([i for i in details["interventions"] if i["action"] == "lease_conflict"]) == 1


def test_legacy_schema_is_migrated_with_lease_fencing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "legacy.db"
    monkeypatch.setenv("TOWNSHIP_DATABASE_PATH", str(db_path))
    from app.database import close_connection

    close_connection()

    # 手工构造缺少 lease_epoch / detail_json 的旧库结构。
    legacy = sqlite3.connect(db_path, isolation_level=None)
    legacy.execute(
        "CREATE TABLE compute_templates (id INTEGER PRIMARY KEY, code TEXT UNIQUE, name TEXT, algorithm TEXT, version INTEGER DEFAULT 1, parameter_schema_json TEXT, default_parameters_json TEXT, max_runtime_seconds INTEGER, max_attempts INTEGER, active INTEGER DEFAULT 1, created_by TEXT, created_at TEXT, updated_at TEXT)"
    )
    legacy.execute(
        "CREATE TABLE compute_tasks (id INTEGER PRIMARY KEY, template_id INTEGER, project_code TEXT, requested_by TEXT, parameters_json TEXT, parameter_digest TEXT, priority INTEGER DEFAULT 50, idempotency_key TEXT, status TEXT DEFAULT 'queued', attempt_count INTEGER DEFAULT 0, max_attempts INTEGER, available_at TEXT, lease_owner TEXT DEFAULT '', lease_expires_at TEXT DEFAULT '', current_result_version INTEGER, last_error_code TEXT DEFAULT '', last_error_message TEXT DEFAULT '', version INTEGER DEFAULT 1, started_at TEXT, finished_at TEXT, created_at TEXT, updated_at TEXT)"
    )
    legacy.execute(
        "CREATE TABLE compute_interventions (id INTEGER PRIMARY KEY, task_id INTEGER, actor TEXT, action TEXT, reason TEXT, before_json TEXT, after_json TEXT, batch_key TEXT DEFAULT '', created_at TEXT)"
    )
    now = START.isoformat(timespec="seconds")
    legacy.execute(
        "INSERT INTO compute_templates(id,code,name,algorithm,parameter_schema_json,default_parameters_json,max_runtime_seconds,max_attempts,created_by,created_at,updated_at) VALUES(1,'solver-a','t','solver-a','{}','{}',300,2,'admin',?,?)",
        (now, now),
    )
    legacy.execute(
        "INSERT INTO compute_tasks(id,template_id,project_code,requested_by,parameters_json,parameter_digest,idempotency_key,status,attempt_count,max_attempts,available_at,lease_owner,lease_expires_at,created_at,updated_at) VALUES(1,1,'p','u','{}','d','legacy-key-000001','running',1,2,?, 'worker-a',?,?,?)",
        (now, now, now, now),
    )
    legacy.close()

    init_db()
    service = ComputeOperationsService(get_connection(), FrozenClock(START + timedelta(seconds=30)))
    result = service.recover_expired()
    assert result == {"recovered": [1], "exhausted": []}
    details = service.get_task(1)
    assert details["lease_epoch"] == 1
    intervention = details["interventions"][-1]
    assert intervention["action"] == "lease_recovery"
    assert intervention["lease_epoch"] == 0
    close_connection()
