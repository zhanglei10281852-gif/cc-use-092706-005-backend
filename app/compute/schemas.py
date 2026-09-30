from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class TemplateCreate(BaseModel):
    code: str = Field(min_length=2, max_length=64, pattern=r"^[a-z0-9][a-z0-9._-]+$")
    name: str = Field(min_length=2, max_length=120)
    algorithm: str = Field(min_length=2, max_length=120)
    parameter_schema: dict[str, dict[str, Any]]
    default_parameters: dict[str, Any] = Field(default_factory=dict)
    max_runtime_seconds: int = Field(default=600, ge=1, le=86400)
    max_attempts: int = Field(default=3, ge=1, le=20)


class QuotaSet(BaseModel):
    subject_type: Literal["user", "role", "project"]
    subject_key: str = Field(min_length=1, max_length=120)
    max_queued: int = Field(default=20, ge=0, le=100000)
    max_running: int = Field(default=4, ge=0, le=10000)
    daily_submissions: int = Field(default=200, ge=0, le=1000000)


class TaskSubmit(BaseModel):
    template_code: str = Field(min_length=2, max_length=64)
    project_code: str = Field(min_length=1, max_length=80)
    requested_by: str = Field(min_length=1, max_length=80)
    parameters: dict[str, Any]
    priority: int = Field(default=50, ge=0, le=100)
    idempotency_key: str = Field(min_length=6, max_length=160)


class TaskClaim(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    capabilities: list[str] = Field(default_factory=list, max_length=100)
    lease_seconds: int = Field(default=60, ge=5, le=3600)
    lease_generation: int | None = Field(default=None, ge=1)


class TaskResult(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    result: dict[str, Any]
    metrics: dict[str, Any] = Field(default_factory=dict)
    lease_generation: int | None = Field(default=None, ge=1)


class TaskFailure(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    error_code: str = Field(min_length=1, max_length=120)
    message: str = Field(min_length=1, max_length=2000)
    retryable: bool = True
    lease_generation: int | None = Field(default=None, ge=1)


class CancelRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)


class RetryRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)
    priority: int | None = Field(default=None, ge=0, le=100)


class PriorityRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)
    priority: int = Field(ge=0, le=100)


class BatchOperation(BaseModel):
    task_ids: list[int] = Field(min_length=1, max_length=200)
    operation: Literal["cancel", "retry", "priority"]
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)
    priority: int | None = Field(default=None, ge=0, le=100)

    @model_validator(mode="after")
    def validate_priority(self) -> "BatchOperation":
        if self.operation == "priority" and self.priority is None:
            raise ValueError("批量调整优先级时必须提供 priority")
        return self
