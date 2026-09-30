from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.compute.repository import ComputeRepository
from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import get_connection, transaction


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


class _LeaseConflict(Exception):
    """租约校验失败的内部信号：携带快照触发外层回滚后落库干预记录。"""

    def __init__(self, snapshot: dict[str, Any], reason: str) -> None:
        super().__init__(reason)
        self.snapshot = snapshot
        self.reason = reason


class ComputeOperationsService:
    """管理计算模板、配额、任务租约、结果版本和人工干预。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = ComputeRepository(self.connection)

    def list_templates(self) -> list[dict[str, Any]]:
        return self.repository.active_templates()

    def create_template(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        self._validate_schema(payload["parameter_schema"], payload["default_parameters"])
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if repository.template_by_code(payload["code"]):
                raise ConflictError("参数模板编码已存在")
            return repository.create_template(
                code=payload["code"], name=payload["name"], algorithm=payload["algorithm"],
                parameter_schema=payload["parameter_schema"], defaults=payload["default_parameters"],
                max_runtime_seconds=payload["max_runtime_seconds"], max_attempts=payload["max_attempts"],
                created_by=actor, now=now,
            )

    def set_quota(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            return ComputeRepository(connection).upsert_quota(actor=actor, now=now, **payload)

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            template = repository.template_by_code(payload["template_code"])
            if template is None or not template["active"]:
                raise NotFoundError("参数模板不存在或已经停用")
            parameters = self._validate_parameters(template, payload["parameters"])
            existing = repository.task_by_idempotency(payload["requested_by"], payload["idempotency_key"])
            parameter_digest = digest(parameters)
            if existing is not None:
                if existing["parameter_digest"] != parameter_digest:
                    raise ConflictError("同一幂等键对应了不同的计算参数")
                return dict(repository.task_by_id(existing["id"]))
            self._check_quota(repository, payload["requested_by"], now_value)
            return repository.create_task(
                template_id=template["id"], project_code=payload["project_code"],
                requested_by=payload["requested_by"], parameters=parameters,
                parameter_digest=parameter_digest, priority=payload["priority"],
                idempotency_key=payload["idempotency_key"], max_attempts=template["max_attempts"], now=now,
            )

    def list_tasks(self, *, status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_tasks(status=status, project_code=project_code, requested_by=requested_by, limit=max(1, min(limit, 500)))

    def get_task(self, task_id: int) -> dict[str, Any]:
        row = self.repository.task_by_id(task_id)
        if row is None:
            raise NotFoundError("计算任务不存在")
        result = dict(row)
        result["results"] = self.repository.result_versions(task_id)
        result["interventions"] = self.repository.interventions(task_id)
        return result

    def claim(self, worker_id: str, capabilities: list[str], lease_seconds: int) -> dict[str, Any] | None:
        now_value = self.clock.now()
        now = to_storage(now_value)
        lease_until = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            candidate = repository.queued_candidate(capabilities, now)
            if candidate is None:
                return None
            # 每次派发都推进租约代次，旧持有者的迟到请求所持 epoch 必然落后。
            cursor = connection.execute(
                "UPDATE compute_tasks SET status='running',attempt_count=attempt_count+1,lease_owner=?,lease_expires_at=?,lease_epoch=lease_epoch+1,started_at=COALESCE(started_at,?),updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                (worker_id, lease_until, now, now, candidate["id"]),
            )
            if cursor.rowcount != 1:
                return None
            return dict(repository.task_by_id(candidate["id"]))

    def heartbeat(self, task_id: int, worker_id: str, lease_seconds: int, lease_epoch: int | None = None) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        try:
            with transaction(immediate=True) as connection:
                repository = ComputeRepository(connection)
                task = repository.task_by_id(task_id)
                self._require_active_lease(task, worker_id, lease_epoch, now)
                # 仅当仍持有“当前且未过期”的租约时才允许续期；不放宽过期时间也不重置代次。
                cursor = connection.execute(
                    "UPDATE compute_tasks SET lease_expires_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running' AND lease_owner=? AND lease_epoch=? AND lease_expires_at<>'' AND lease_expires_at>=?",
                    (expires, now, task_id, worker_id, task["lease_epoch"], now),
                )
                if cursor.rowcount != 1:
                    self._raise_conflict(repository.task_by_id(task_id), "租约状态在提交前已改变")
                return dict(repository.task_by_id(task_id))
        except _LeaseConflict as conflict:
            raise self._reject_stale_lease(task_id, worker_id, lease_epoch, "renew_lease", conflict)

    def complete(self, task_id: int, worker_id: str, result: dict[str, Any], metrics: dict[str, Any], lease_epoch: int | None = None) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        try:
            with transaction(immediate=True) as connection:
                repository = ComputeRepository(connection)
                task = repository.task_by_id(task_id)
                if task is None:
                    raise NotFoundError("计算任务不存在")
                self._require_active_lease(task, worker_id, lease_epoch, now)
                version = int(connection.execute("SELECT COALESCE(MAX(version),0)+1 FROM compute_results WHERE task_id=?", (task_id,)).fetchone()[0])
                # 行级 CAS：只有当前代次且租约未过期的持有者能终结任务并落结果版本。
                cursor = connection.execute(
                    "UPDATE compute_tasks SET status='succeeded',current_result_version=?,lease_owner='',lease_expires_at='',lease_epoch=lease_epoch+1,finished_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running' AND lease_owner=? AND lease_epoch=? AND lease_expires_at<>'' AND lease_expires_at>=?",
                    (version, now, now, task_id, worker_id, task["lease_epoch"], now),
                )
                if cursor.rowcount != 1:
                    self._raise_conflict(repository.task_by_id(task_id), "租约状态在提交前已改变")
                connection.execute(
                    "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                    (task_id, version, json.dumps(result, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), digest({"result": result, "metrics": metrics}), worker_id, now),
                )
                return dict(repository.task_by_id(task_id))
        except _LeaseConflict as conflict:
            raise self._reject_stale_lease(task_id, worker_id, lease_epoch, "submit_result", conflict)

    def fail(self, task_id: int, worker_id: str, error_code: str, message: str, retryable: bool, lease_epoch: int | None = None) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        try:
            with transaction(immediate=True) as connection:
                repository = ComputeRepository(connection)
                task = repository.task_by_id(task_id)
                if task is None:
                    raise NotFoundError("计算任务不存在")
                self._require_active_lease(task, worker_id, lease_epoch, now)
                can_retry = retryable and int(task["attempt_count"]) < int(task["max_attempts"])
                status = "queued" if can_retry else "failed"
                delay = min(300, 2 ** max(0, int(task["attempt_count"]) - 1)) if can_retry else 0
                available = to_storage(now_value + timedelta(seconds=delay))
                # 行级 CAS + 无论重试或终结都推进代次并释放租约，旧持有者之后的请求一律冲突。
                cursor = connection.execute(
                    "UPDATE compute_tasks SET status=?,available_at=?,lease_owner='',lease_expires_at='',lease_epoch=lease_epoch+1,last_error_code=?,last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running' AND lease_owner=? AND lease_epoch=? AND lease_expires_at<>'' AND lease_expires_at>=?",
                    (status, available, error_code, message[:2000], None if can_retry else now, now, task_id, worker_id, task["lease_epoch"], now),
                )
                if cursor.rowcount != 1:
                    self._raise_conflict(repository.task_by_id(task_id), "租约状态在提交前已改变")
                return dict(repository.task_by_id(task_id))
        except _LeaseConflict as conflict:
            raise self._reject_stale_lease(task_id, worker_id, lease_epoch, "report_failure", conflict)

    def cancel(self, task_id: int, actor: str, reason: str, batch_key: str = "") -> dict[str, Any]:
        return self._intervene(task_id, actor, reason, "cancel", batch_key, self._cancel_mutation)

    def retry(self, task_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"failed", "cancelled"}:
                raise ConflictError("只有失败或已取消任务可以人工重试")
            chosen = task["priority"] if priority is None else priority
            connection.execute("UPDATE compute_tasks SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?", (chosen, now, now, task["id"]))
        return self._intervene(task_id, actor, reason, "retry", batch_key, mutate)

    def set_priority(self, task_id: int, actor: str, reason: str, priority: int, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"queued", "running"}:
                raise ConflictError("只有排队或运行中的任务可以调整优先级")
            connection.execute("UPDATE compute_tasks SET priority=?,updated_at=?,version=version+1 WHERE id=?", (priority, now, task["id"]))
        return self._intervene(task_id, actor, reason, "priority", batch_key, mutate)

    def batch_operation(self, payload: dict[str, Any]) -> dict[str, Any]:
        batch_key = digest({"actor": payload["actor"], "task_ids": payload["task_ids"], "operation": payload["operation"], "reason": payload["reason"]})
        succeeded: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for task_id in list(dict.fromkeys(payload["task_ids"])):
            try:
                if payload["operation"] == "cancel":
                    value = self.cancel(task_id, payload["actor"], payload["reason"], batch_key)
                elif payload["operation"] == "retry":
                    value = self.retry(task_id, payload["actor"], payload["reason"], payload.get("priority"), batch_key)
                else:
                    value = self.set_priority(task_id, payload["actor"], payload["reason"], int(payload["priority"]), batch_key)
                succeeded.append({"task_id": task_id, "status": value["status"], "version": value["version"]})
            except (ConflictError, NotFoundError) as exc:
                failed.append({"task_id": task_id, "code": exc.code, "message": exc.message})
        return {"batch_key": batch_key, "succeeded": succeeded, "failed": failed}

    def recover_expired(self, actor: str = "recovery-worker") -> dict[str, Any]:
        now = to_storage(self.clock.now())
        recovered: list[int] = []
        exhausted: list[int] = []
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            # 候选集只在同一写事务内作为快照；最终是否接管由条件 UPDATE 的 CAS 决定，
            # 因此与心跳交错时：心跳先提交则其 lease_expires_at 已续期，CAS 的 expires<? 不成立；
            # 恢复先提交则状态离开 running，迟到心跳的 epoch/状态条件不再成立。
            rows = connection.execute(
                "SELECT * FROM compute_tasks WHERE status='running' AND lease_expires_at<>'' AND lease_expires_at<? ORDER BY id",
                (now,),
            ).fetchall()
            for task in rows:
                before = dict(task)
                if int(task["attempt_count"]) < int(task["max_attempts"]):
                    status, finished_at = "queued", None
                    recovered.append(int(task["id"]))
                else:
                    status, finished_at = "failed", now
                    exhausted.append(int(task["id"]))
                cursor = connection.execute(
                    "UPDATE compute_tasks SET status=?,lease_owner='',lease_expires_at='',lease_epoch=lease_epoch+1,available_at=?,last_error_code='lease_expired',last_error_message='工作者租约已过期',finished_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running' AND lease_expires_at<>'' AND lease_expires_at<? AND lease_owner=? AND lease_epoch=?",
                    (status, now, finished_at, now, task["id"], now, task["lease_owner"], task["lease_epoch"]),
                )
                # CAS 未命中说明该任务已被并发心跳/接管处理：恢复必须幂等，跳过且不再记干预。
                if cursor.rowcount != 1:
                    recovered = [item for item in recovered if item != int(task["id"])]
                    exhausted = [item for item in exhausted if item != int(task["id"])]
                    continue
                after = dict(repository.task_by_id(task["id"]))
                # 同一代次只记一次恢复：已存在 lease_recovery 干预则幂等跳过。
                if not repository.intervention_exists(task["id"], "lease_recovery", int(task["lease_epoch"])):
                    repository.add_intervention(task_id=task["id"], actor=actor, action="lease_recovery", reason="租约过期自动恢复", before=before, after=after, batch_key="", now=now, lease_epoch=int(task["lease_epoch"]))
        return {"recovered": recovered, "exhausted": exhausted}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM compute_tasks WHERE status='queued'").fetchone()[0]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "templates": len(self.repository.active_templates())}

    def _intervene(self, task_id: int, actor: str, reason: str, action: str, batch_key: str, mutation: Callable[[sqlite3.Connection, sqlite3.Row, str], None]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            before = dict(task)
            mutation(connection, task, now)
            after = dict(repository.task_by_id(task_id))
            repository.add_intervention(task_id=task_id, actor=actor, action=action, reason=reason, before=before, after=after, batch_key=batch_key, now=now)
            return after

    def _require_active_lease(self, task: sqlite3.Row | None, worker_id: str, lease_epoch: int | None, now: str) -> None:
        """只有仍持有当前、未过期租约的工作者可以续期或提交结果。"""
        if task is None:
            raise NotFoundError("计算任务不存在")
        snapshot = dict(task)
        if task["status"] != "running":
            self._raise_conflict(snapshot, f"任务当前状态为 {task['status']}，不再接受工作者写入")
        if task["lease_owner"] != worker_id:
            self._raise_conflict(snapshot, "任务已不属于该工作者")
        if lease_epoch is not None and int(task["lease_epoch"]) != int(lease_epoch):
            self._raise_conflict(snapshot, f"租约代次已推进到 {task['lease_epoch']}，请求携带的 {lease_epoch} 已失效")
        if not task["lease_expires_at"] or task["lease_expires_at"] < now:
            self._raise_conflict(snapshot, "工作者租约已过期，需等待恢复后重新领取")

    @staticmethod
    def _raise_conflict(snapshot: sqlite3.Row | dict[str, Any] | None, reason: str) -> None:
        raise _LeaseConflict(dict(snapshot) if snapshot is not None else {}, reason)

    def _reject_stale_lease(self, task_id: int, worker_id: str, lease_epoch: int | None, action: str, conflict: _LeaseConflict) -> ConflictError:
        """迟到请求落一条可追踪的 lease_conflict 干预记录，再抛出稳定的 409。"""
        now = to_storage(self.clock.now())
        try:
            with transaction(immediate=True) as connection:
                repository = ComputeRepository(connection)
                current = repository.task_by_id(task_id)
                if current is not None:
                    repository.add_intervention(
                        task_id=task_id,
                        actor=worker_id,
                        action="lease_conflict",
                        reason=conflict.reason,
                        before=conflict.snapshot,
                        after=dict(current),
                        batch_key="",
                        now=now,
                        lease_epoch=int(current["lease_epoch"]),
                        detail={
                            "requested_action": action,
                            "presented_epoch": None if lease_epoch is None else int(lease_epoch),
                            "current_epoch": int(current["lease_epoch"]),
                            "current_owner": current["lease_owner"],
                            "current_status": current["status"],
                        },
                    )
        except sqlite3.Error:
            pass
        return ConflictError("租约已失效或不属于当前工作者", context={"action": action})

    @staticmethod
    def _cancel_mutation(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
        if task["status"] not in {"queued", "running"}:
            raise ConflictError("当前任务状态不允许取消")
        status = "cancel_requested" if task["status"] == "running" else "cancelled"
        connection.execute("UPDATE compute_tasks SET status=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?", (status, None if status == "cancel_requested" else now, now, task["id"]))

    def _check_quota(self, repository: ComputeRepository, requested_by: str, now: datetime) -> None:
        quota = repository.quota("user", requested_by)
        if quota is None:
            return
        states = repository.count_user_states(requested_by)
        if states.get("queued", 0) >= int(quota["max_queued"]):
            raise ConflictError("用户排队任务配额已用尽")
        if states.get("running", 0) >= int(quota["max_running"]):
            raise ConflictError("用户运行任务配额已用尽")
        day_start = to_storage(now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0))
        if repository.count_user_submissions_since(requested_by, day_start) >= int(quota["daily_submissions"]):
            raise ConflictError("用户当日提交配额已用尽")

    @staticmethod
    def _validate_schema(schema: dict[str, dict[str, Any]], defaults: dict[str, Any]) -> None:
        if not schema:
            raise ValidationError("参数模板至少包含一个参数")
        allowed = {"integer", "number", "string", "boolean"}
        for name, rule in schema.items():
            if not name or not isinstance(rule, dict) or rule.get("type") not in allowed:
                raise ValidationError(f"参数 {name or '<empty>'} 的规则不合法")
        if set(defaults) - set(schema):
            raise ValidationError("默认值包含未声明参数")

    def _validate_parameters(self, template: sqlite3.Row, supplied: dict[str, Any]) -> dict[str, Any]:
        schema = json.loads(template["parameter_schema_json"])
        values = {**json.loads(template["default_parameters_json"]), **supplied}
        unknown = set(values) - set(schema)
        if unknown:
            raise ValidationError("包含模板未声明的参数", context={"parameters": sorted(unknown)})
        normalized: dict[str, Any] = {}
        for name, rule in schema.items():
            if name not in values:
                if rule.get("required"):
                    raise ValidationError(f"缺少必填参数：{name}")
                continue
            value = values[name]
            kind = rule["type"]
            valid = {"integer": isinstance(value, int) and not isinstance(value, bool), "number": isinstance(value, (int, float)) and not isinstance(value, bool), "string": isinstance(value, str), "boolean": isinstance(value, bool)}[kind]
            if not valid:
                raise ValidationError(f"参数 {name} 类型不正确")
            if rule.get("minimum") is not None and value < rule["minimum"]:
                raise ValidationError(f"参数 {name} 小于允许的最小值")
            if rule.get("maximum") is not None and value > rule["maximum"]:
                raise ValidationError(f"参数 {name} 大于允许的最大值")
            if rule.get("choices") and value not in rule["choices"]:
                raise ValidationError(f"参数 {name} 不在允许的选项中")
            normalized[name] = value
        return normalized
