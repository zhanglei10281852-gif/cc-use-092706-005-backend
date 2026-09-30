from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from app.core.clock import to_storage, utc_now

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "township.db"
_local = threading.local()

SCHEMA = r''' 
CREATE TABLE IF NOT EXISTS departments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    manager TEXT NOT NULL,
    phone TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    display_name TEXT NOT NULL,
    email TEXT,
    phone TEXT,
    department_id INTEGER REFERENCES departments(id),
    status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','disabled','locked')),
    failed_login_count INTEGER NOT NULL DEFAULT 0,
    locked_until TEXT,
    password_changed_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS roles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    is_system INTEGER NOT NULL DEFAULT 0 CHECK(is_system IN (0,1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS permissions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    resource TEXT NOT NULL,
    action TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS role_permissions (
    role_id INTEGER NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    permission_id INTEGER NOT NULL REFERENCES permissions(id) ON DELETE CASCADE,
    granted_at TEXT NOT NULL,
    PRIMARY KEY(role_id, permission_id)
);

CREATE TABLE IF NOT EXISTS user_roles (
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role_id INTEGER NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    assigned_by INTEGER REFERENCES users(id),
    assigned_at TEXT NOT NULL,
    PRIMARY KEY(user_id, role_id)
);

CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_digest TEXT NOT NULL UNIQUE,
    issued_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    revoked_at TEXT,
    revoke_reason TEXT,
    client_label TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    actor_user_id INTEGER REFERENCES users(id),
    actor_name TEXT NOT NULL,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT,
    outcome TEXT NOT NULL CHECK(outcome IN ('success','denied','failure')),
    before_json TEXT,
    after_json TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    correlation_id TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_resource ON audit_events(resource_type, resource_id);

CREATE TABLE IF NOT EXISTS idempotency_records (
    scope TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    response_json TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(scope, idempotency_key)
);

CREATE TABLE IF NOT EXISTS residents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    id_card TEXT NOT NULL UNIQUE,
    gender TEXT NOT NULL CHECK(gender IN ('男', '女')),
    birth_date TEXT NOT NULL,
    phone TEXT,
    address TEXT NOT NULL,
    village TEXT NOT NULL,
    household_head TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS affairs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    category TEXT NOT NULL CHECK(category IN ('户籍','社保','医保','低保','建房','计生','其他')),
    applicant_id INTEGER NOT NULL REFERENCES residents(id),
    description TEXT,
    status TEXT NOT NULL DEFAULT '待受理' CHECK(status IN ('待受理','办理中','已办结','已退回')),
    department_id INTEGER REFERENCES departments(id),
    handler TEXT,
    result TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS announcements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    category TEXT NOT NULL CHECK(category IN ('通知','公告','政策','公示')),
    publisher TEXT NOT NULL,
    is_pinned INTEGER NOT NULL DEFAULT 0 CHECK(is_pinned IN (0,1)),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS petitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL CHECK(type IN ('投诉举报','意见建议','求助咨询','信息公开申请')),
    target TEXT NOT NULL,
    content TEXT NOT NULL,
    demand TEXT,
    contact TEXT,
    is_anonymous INTEGER NOT NULL DEFAULT 0 CHECK(is_anonymous IN (0,1)),
    status TEXT NOT NULL DEFAULT '待签收' CHECK(status IN ('待签收','待分派','办理中','待审核','已办结','退回重办','复查中','复查完结')),
    department_id INTEGER REFERENCES departments(id),
    deadline TEXT,
    process_result TEXT,
    review_opinion TEXT,
    review_result TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS petition_urges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    petition_id INTEGER NOT NULL REFERENCES petitions(id) ON DELETE CASCADE,
    reason TEXT NOT NULL,
    operator TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS petition_flow_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    petition_id INTEGER NOT NULL REFERENCES petitions(id) ON DELETE CASCADE,
    action TEXT NOT NULL,
    operator TEXT,
    remark TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS department_memberships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    department_id INTEGER NOT NULL REFERENCES departments(id),
    title TEXT NOT NULL DEFAULT '',
    is_primary INTEGER NOT NULL DEFAULT 0 CHECK(is_primary IN (0,1)),
    starts_at TEXT NOT NULL,
    ends_at TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(user_id, department_id, starts_at)
);

CREATE TABLE IF NOT EXISTS background_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type TEXT NOT NULL,
    deduplication_key TEXT NOT NULL UNIQUE,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','running','completed','failed','cancelled')),
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TEXT NOT NULL,
    locked_at TEXT,
    locked_by TEXT,
    result_json TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_ready ON background_jobs(status, available_at);

CREATE TABLE IF NOT EXISTS compute_templates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    algorithm TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    parameter_schema_json TEXT NOT NULL,
    default_parameters_json TEXT NOT NULL DEFAULT '{}',
    max_runtime_seconds INTEGER NOT NULL CHECK(max_runtime_seconds > 0),
    max_attempts INTEGER NOT NULL CHECK(max_attempts > 0),
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS compute_quotas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_type TEXT NOT NULL CHECK(subject_type IN ('user','role','project')),
    subject_key TEXT NOT NULL,
    max_queued INTEGER NOT NULL CHECK(max_queued >= 0),
    max_running INTEGER NOT NULL CHECK(max_running >= 0),
    daily_submissions INTEGER NOT NULL CHECK(daily_submissions >= 0),
    updated_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(subject_type, subject_key)
);
CREATE TABLE IF NOT EXISTS compute_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    template_id INTEGER NOT NULL REFERENCES compute_templates(id) ON DELETE RESTRICT,
    project_code TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    parameters_json TEXT NOT NULL,
    parameter_digest TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 50 CHECK(priority BETWEEN 0 AND 100),
    idempotency_key TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','running','cancel_requested','cancelled','succeeded','failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL CHECK(max_attempts > 0),
    available_at TEXT NOT NULL,
    lease_owner TEXT NOT NULL DEFAULT '',
    lease_expires_at TEXT NOT NULL DEFAULT '',
    lease_epoch INTEGER NOT NULL DEFAULT 0,
    current_result_version INTEGER,
    last_error_code TEXT NOT NULL DEFAULT '',
    last_error_message TEXT NOT NULL DEFAULT '',
    version INTEGER NOT NULL DEFAULT 1,
    started_at TEXT,
    finished_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(requested_by, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_compute_tasks_queue ON compute_tasks(status,priority DESC,available_at,created_at);
CREATE INDEX IF NOT EXISTS idx_compute_tasks_owner ON compute_tasks(requested_by,status,created_at);
CREATE TABLE IF NOT EXISTS compute_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL REFERENCES compute_tasks(id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    result_json TEXT NOT NULL,
    metrics_json TEXT NOT NULL DEFAULT '{}',
    result_digest TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(task_id, version)
);
CREATE TABLE IF NOT EXISTS compute_interventions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL REFERENCES compute_tasks(id) ON DELETE CASCADE,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    reason TEXT NOT NULL,
    before_json TEXT NOT NULL,
    after_json TEXT NOT NULL,
    batch_key TEXT NOT NULL DEFAULT '',
    lease_epoch INTEGER NOT NULL DEFAULT 0,
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_compute_interventions_task ON compute_interventions(task_id,id);
'''

PERMISSIONS = [
    ("users.read", "查看用户", "users", "read"),
    ("users.write", "维护用户", "users", "write"),
    ("roles.read", "查看角色", "roles", "read"),
    ("roles.write", "维护角色", "roles", "write"),
    ("departments.read", "查看部门", "departments", "read"),
    ("departments.write", "维护部门", "departments", "write"),
    ("residents.read", "查看居民", "residents", "read"),
    ("residents.write", "维护居民", "residents", "write"),
    ("affairs.read", "查看事务", "affairs", "read"),
    ("affairs.write", "办理事务", "affairs", "write"),
    ("petitions.read", "查看信访", "petitions", "read"),
    ("petitions.write", "办理信访", "petitions", "write"),
    ("announcements.write", "维护公告", "announcements", "write"),
    ("audit.read", "查看审计", "audit", "read"),
    ("jobs.run", "执行后台任务", "jobs", "run"),
]


def database_path() -> Path:
    raw = os.getenv("TOWNSHIP_DATABASE_PATH", str(DEFAULT_DB_PATH))
    return Path(raw).expanduser().resolve()


def _create_connection() -> sqlite3.Connection:
    path = database_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, check_same_thread=False, timeout=30, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    return connection


def get_connection() -> sqlite3.Connection:
    connection = getattr(_local, "connection", None)
    if connection is None:
        connection = _create_connection()
        _local.connection = connection
    return connection


def close_connection() -> None:
    connection = getattr(_local, "connection", None)
    if connection is not None:
        connection.close()
        _local.connection = None


@contextmanager
def transaction(*, immediate: bool = False) -> Iterator[sqlite3.Connection]:
    connection = get_connection()
    connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield connection
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()


def _table_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in connection.execute(f"PRAGMA table_info({table})").fetchall()}


def _ensure_schema_revisions(connection: sqlite3.Connection) -> None:
    """对既有库补齐后加列；全新库已由 SCHEMA 包含，所有改动均可重复执行。"""
    task_columns = _table_columns(connection, "compute_tasks")
    if "lease_epoch" not in task_columns:
        connection.execute("ALTER TABLE compute_tasks ADD COLUMN lease_epoch INTEGER NOT NULL DEFAULT 0")
    intervention_columns = _table_columns(connection, "compute_interventions")
    if "lease_epoch" not in intervention_columns:
        connection.execute("ALTER TABLE compute_interventions ADD COLUMN lease_epoch INTEGER NOT NULL DEFAULT 0")
    if "detail_json" not in intervention_columns:
        connection.execute("ALTER TABLE compute_interventions ADD COLUMN detail_json TEXT NOT NULL DEFAULT '{}'")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_compute_interventions_lease ON compute_interventions(task_id,action,lease_epoch)")


def init_db() -> None:
    now = to_storage(utc_now())
    with transaction(immediate=True) as connection:
        connection.executescript(SCHEMA)
        _ensure_schema_revisions(connection)
        for code, name, resource, action in PERMISSIONS:
            connection.execute(
                "INSERT OR IGNORE INTO permissions(code,name,resource,action) VALUES(?,?,?,?)",
                (code, name, resource, action),
            )
        connection.execute(
            "INSERT OR IGNORE INTO roles(code,name,description,is_system,created_at,updated_at) VALUES('administrator','系统管理员','拥有全部系统权限',1,?,?)",
            (now, now),
        )
        connection.execute(
            "INSERT OR IGNORE INTO roles(code,name,description,is_system,created_at,updated_at) VALUES('clerk','综合经办员','可处理居民、事务与信访业务',1,?,?)",
            (now, now),
        )
        connection.execute(
            "INSERT OR IGNORE INTO roles(code,name,description,is_system,created_at,updated_at) VALUES('auditor','审计查看员','只读查看业务与审计记录',1,?,?)",
            (now, now),
        )
        administrator = connection.execute("SELECT id FROM roles WHERE code='administrator'").fetchone()[0]
        connection.execute(
            "INSERT OR IGNORE INTO role_permissions(role_id,permission_id,granted_at) SELECT ?,id,? FROM permissions",
            (administrator, now),
        )


def migrate_db() -> None:
    init_db()
