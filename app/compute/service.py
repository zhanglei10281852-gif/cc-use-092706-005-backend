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
            cursor = connection.execute(
                "UPDATE compute_tasks SET status='running',attempt_count=attempt_count+1,lease_owner=?,lease_expires_at=?,lease_generation=lease_generation+1,started_at=COALESCE(started_at,?),updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                (worker_id, lease_until, now, now, candidate["id"]),
            )
            if cursor.rowcount != 1:
                return None
            return dict(repository.task_by_id(candidate["id"]))

    def heartbeat(self, task_id: int, worker_id: str, lease_seconds: int, lease_generation: int | None = None) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        conflict: ConflictError | None = None
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            # 只有仍持有“当前且未过期”租约（代数一致）的工作者才能续租。
            cursor = connection.execute(
                "UPDATE compute_tasks SET lease_expires_at=?,updated_at=?,version=version+1 "
                "WHERE id=? AND status='running' AND lease_owner=? AND lease_expires_at<>'' AND lease_expires_at>=? "
                "AND (? IS NULL OR lease_generation=?)",
                (expires, now, task_id, worker_id, now, lease_generation, lease_generation),
            )
            if cursor.rowcount != 1:
                task = repository.task_by_id(task_id)
                if task is None:
                    raise NotFoundError("计算任务不存在")
                self._record_lease_conflict(connection, task, worker_id, "heartbeat", now)
                conflict = ConflictError("任务未由当前工作者持有或租约已过期")
                outcome: dict[str, Any] | None = None
            else:
                outcome = dict(repository.task_by_id(task_id))
        if conflict is not None:
            raise conflict
        return outcome

    def complete(self, task_id: int, worker_id: str, result: dict[str, Any], metrics: dict[str, Any], lease_generation: int | None = None) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        conflict: ConflictError | None = None
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            result_version = int(connection.execute(
                "SELECT COALESCE(MAX(version),0)+1 FROM compute_results WHERE task_id=?", (task_id,)
            ).fetchone()[0])
            # 条件更新即权威判定：状态、持有者、租约是否仍在有效期、租约代数缺一不可，
            # 与恢复扫描并发时二者至多一方成功。
            cursor = connection.execute(
                "UPDATE compute_tasks SET status='succeeded',current_result_version=?,lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 "
                "WHERE id=? AND status='running' AND lease_owner=? AND lease_expires_at<>'' AND lease_expires_at>=? "
                "AND (? IS NULL OR lease_generation=?)",
                (result_version, now, now, task_id, worker_id, now, lease_generation, lease_generation),
            )
            if cursor.rowcount != 1:
                self._record_lease_conflict(connection, task, worker_id, "complete", now)
                conflict = ConflictError("任务未由当前工作者持有或租约已过期")
                outcome: dict[str, Any] | None = None
            else:
                connection.execute(
                    "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                    (task_id, result_version, json.dumps(result, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), digest({"result": result, "metrics": metrics}), worker_id, now),
                )
                outcome = dict(repository.task_by_id(task_id))
        if conflict is not None:
            raise conflict
        return outcome

    def fail(self, task_id: int, worker_id: str, error_code: str, message: str, retryable: bool, lease_generation: int | None = None) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        conflict: ConflictError | None = None
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            can_retry = retryable and int(task["attempt_count"]) < int(task["max_attempts"])
            status = "queued" if can_retry else "failed"
            delay = min(300, 2 ** max(0, int(task["attempt_count"]) - 1)) if can_retry else 0
            available = to_storage(now_value + timedelta(seconds=delay))
            cursor = connection.execute(
                "UPDATE compute_tasks SET status=?,available_at=?,lease_owner='',lease_expires_at='',last_error_code=?,last_error_message=?,finished_at=?,updated_at=?,version=version+1 "
                "WHERE id=? AND status='running' AND lease_owner=? AND lease_expires_at<>'' AND lease_expires_at>=? "
                "AND (? IS NULL OR lease_generation=?)",
                (status, available, error_code, message[:2000], None if can_retry else now, now, task_id, worker_id, now, lease_generation, lease_generation),
            )
            if cursor.rowcount != 1:
                self._record_lease_conflict(connection, task, worker_id, "fail", now)
                conflict = ConflictError("任务未由当前工作者持有或租约已过期")
                outcome = None
            else:
                outcome = dict(repository.task_by_id(task_id))
        if conflict is not None:
            raise conflict
        return outcome

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
            # 扫描候选行只是候选；真正的状态推进由下面带完整租约快照的条件 UPDATE 决定，
            # 因此与心跳/提交并发时，只有一方能让 WHERE 命中（串行化）。
            rows = connection.execute(
                "SELECT * FROM compute_tasks WHERE status='running' AND lease_expires_at<>'' AND lease_expires_at<? ORDER BY id",
                (now,),
            ).fetchall()
            for task in rows:
                before = dict(task)
                if int(task["attempt_count"]) < int(task["max_attempts"]):
                    status, finished_at, available_at, bucket = "queued", None, now, recovered
                else:
                    status, finished_at, available_at, bucket = "failed", now, now, exhausted
                cursor = connection.execute(
                    "UPDATE compute_tasks SET status=?,lease_owner='',lease_expires_at='',available_at=?,"
                    "last_error_code='lease_expired',last_error_message='工作者租约已过期',finished_at=?,updated_at=?,version=version+1 "
                    "WHERE id=? AND status='running' AND lease_owner=? AND lease_generation=? AND lease_expires_at=? AND lease_expires_at<>'' AND lease_expires_at<?",
                    (status, available_at, finished_at, now, task["id"], task["lease_owner"], int(task["lease_generation"]), task["lease_expires_at"], now),
                )
                if cursor.rowcount != 1:
                    # 扫描后该行已被持有者合法续租或已终结：本次恢复对该行幂等跳过，
                    # 不重复推进版本，也不重复写干预记录。
                    continue
                bucket.append(int(task["id"]))
                after = dict(repository.task_by_id(task["id"]))
                repository.add_intervention(
                    task_id=task["id"], actor=actor, action="lease_recovery", reason="租约过期自动恢复",
                    before=before, after=after, batch_key="", now=now,
                )
        return {"recovered": recovered, "exhausted": exhausted}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM compute_tasks WHERE status='queued'").fetchone()[0]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "templates": len(self.repository.active_templates())}

    def _record_lease_conflict(self, connection: sqlite3.Connection, task: sqlite3.Row, worker_id: str, action: str, now: str) -> None:
        """旧持有者的迟到请求（续租/提交/失败）被拒时留下可追踪、可重复执行的干预记录。

        以 (任务, 租约代数, 工作者, 动作) 生成去重键，同一迟到工作者在同一租约代际内的
        重复冲突只落一条记录，保证该动作本身幂等、不产生干扰性噪声。
        """
        repository = ComputeRepository(connection)
        dedup_key = digest({"kind": "lease_conflict", "task_id": int(task["id"]), "lease_generation": int(task["lease_generation"]), "worker": worker_id, "action": action})
        existing = connection.execute(
            "SELECT 1 FROM compute_interventions WHERE task_id=? AND action='lease_conflict' AND batch_key=? LIMIT 1",
            (task["id"], dedup_key),
        ).fetchone()
        if existing is not None:
            return
        snapshot = dict(task)
        reason = f"工作者 {worker_id} 的{action}请求与当前租约状态冲突（状态={task['status']}，持有者={task['lease_owner'] or '<none>'}，租约代数={int(task['lease_generation'])}）"
        repository.add_intervention(
            task_id=int(task["id"]), actor=worker_id, action="lease_conflict", reason=reason,
            before=snapshot, after=snapshot, batch_key=dedup_key, now=now,
        )

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
