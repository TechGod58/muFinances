from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Mapping

from .audit import AuditService
from .base import DatabaseConnection, ServiceContext, ValidationError, fetch_all, require_fields


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    DEAD_LETTER = "dead_letter"


@dataclass(frozen=True)
class BackoffPolicy:
    max_attempts: int = 3
    base_seconds: int = 30
    max_seconds: int = 900

    def next_delay(self, attempt: int) -> int:
        if attempt < 1:
            attempt = 1
        return min(self.max_seconds, self.base_seconds * (2 ** (attempt - 1)))


@dataclass(frozen=True)
class JobRecord:
    job_id: str
    job_type: str
    status: JobStatus
    attempts: int = 0
    payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class JobHealth:
    queued: int
    running: int
    failed: int
    dead_letter: int
    healthy: bool


class BackgroundJobService:
    def __init__(
        self,
        db: DatabaseConnection,
        audit: AuditService | None = None,
        backoff: BackoffPolicy | None = None,
    ):
        self.db = db
        self.audit = audit or AuditService(db)
        self.backoff = backoff or BackoffPolicy()

    def enqueue(self, context: ServiceContext, payload: Mapping[str, Any]) -> str:
        require_fields(payload, ("job_id", "job_type"))
        job_id = str(payload["job_id"])
        self.db.execute(
            """
            INSERT INTO background_jobs (
                job_id, job_type, status, payload_json, attempts, run_after, created_by, created_at
            ) VALUES (?, ?, 'queued', ?, 0, ?, ?, ?)
            """,
            (
                job_id,
                payload["job_type"],
                json.dumps(dict(payload.get("payload", {})), default=str, sort_keys=True),
                payload.get("run_after") or datetime.now(timezone.utc).isoformat(),
                context.user_id,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.log(job_id, "queued", "Job queued", {"job_type": payload["job_type"]})
        self.audit.record(context, "job.enqueue", "background_job", job_id, payload)
        return job_id

    def lease_next(self, worker_id: str, job_types: tuple[str, ...] = ()) -> JobRecord | None:
        type_filter = "" if not job_types else f"AND job_type IN ({','.join(['?'] * len(job_types))})"
        params: tuple[Any, ...] = (*job_types, datetime.now(timezone.utc).isoformat())
        cursor = self.db.execute(
            f"""
            SELECT job_id, job_type, status, attempts, payload_json
            FROM background_jobs
            WHERE status = 'queued'
              {type_filter}
              AND run_after <= ?
            ORDER BY priority DESC, created_at
            LIMIT 1
            """,
            params,
        )
        rows = fetch_all(cursor)
        if not rows:
            return None

        row = rows[0]
        attempts = int(row.get("attempts") or 0) + 1
        self.db.execute(
            """
            UPDATE background_jobs
            SET status = 'running', worker_id = ?, attempts = ?, leased_at = ?
            WHERE job_id = ? AND status = 'queued'
            """,
            (worker_id, attempts, datetime.now(timezone.utc).isoformat(), row["job_id"]),
        )
        self.log(str(row["job_id"]), "leased", "Job leased", {"worker_id": worker_id, "attempts": attempts})
        return JobRecord(
            job_id=str(row["job_id"]),
            job_type=str(row["job_type"]),
            status=JobStatus.RUNNING,
            attempts=attempts,
            payload=json.loads(row.get("payload_json") or "{}"),
        )

    def complete(self, context: ServiceContext, job_id: str, result: Mapping[str, Any] | None = None) -> None:
        self.db.execute(
            """
            UPDATE background_jobs
            SET status = 'completed', result_json = ?, completed_at = ?
            WHERE job_id = ? AND status = 'running'
            """,
            (json.dumps(dict(result or {}), default=str, sort_keys=True), datetime.now(timezone.utc).isoformat(), job_id),
        )
        self.log(job_id, "completed", "Job completed", result or {})
        self.audit.record(context, "job.complete", "background_job", job_id, result or {})

    def fail(self, context: ServiceContext, job_id: str, error: str, attempts: int) -> JobStatus:
        if attempts >= self.backoff.max_attempts:
            self.db.execute(
                """
                UPDATE background_jobs
                SET status = 'dead_letter', last_error = ?, failed_at = ?
                WHERE job_id = ?
                """,
                (error, datetime.now(timezone.utc).isoformat(), job_id),
            )
            status = JobStatus.DEAD_LETTER
        else:
            delay = self.backoff.next_delay(attempts)
            run_after = datetime.now(timezone.utc) + timedelta(seconds=delay)
            self.db.execute(
                """
                UPDATE background_jobs
                SET status = 'queued', last_error = ?, run_after = ?
                WHERE job_id = ?
                """,
                (error, run_after.isoformat(), job_id),
            )
            status = JobStatus.QUEUED
        self.log(job_id, "failed", error, {"next_status": status.value, "attempts": attempts})
        self.audit.record(context, "job.fail", "background_job", job_id, {"error": error, "status": status.value})
        return status

    def cancel(self, context: ServiceContext, job_id: str, reason: str) -> None:
        self.db.execute(
            """
            UPDATE background_jobs
            SET status = 'cancelled', cancellation_reason = ?, completed_at = ?
            WHERE job_id = ? AND status IN ('queued', 'running')
            """,
            (reason, datetime.now(timezone.utc).isoformat(), job_id),
        )
        self.log(job_id, "cancelled", reason, {})
        self.audit.record(context, "job.cancel", "background_job", job_id, {"reason": reason})

    def heartbeat(self, worker_id: str, metadata: Mapping[str, Any] | None = None) -> None:
        self.db.execute(
            """
            INSERT INTO background_worker_heartbeats (worker_id, metadata_json, seen_at)
            VALUES (?, ?, ?)
            """,
            (
                worker_id,
                json.dumps(dict(metadata or {}), default=str, sort_keys=True),
                datetime.now(timezone.utc).isoformat(),
            ),
        )

    def diagnostics(self) -> list[dict[str, Any]]:
        return fetch_all(
            self.db.execute(
                """
                SELECT job_id, job_type, status, attempts, last_error, worker_id, created_at, run_after
                FROM background_jobs
                ORDER BY created_at DESC
                LIMIT 200
                """
            )
        )

    def health_probe(self) -> JobHealth:
        rows = fetch_all(
            self.db.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM background_jobs
                GROUP BY status
                """
            )
        )
        counts = {str(row["status"]): int(row["count"]) for row in rows}
        dead_letter = counts.get(JobStatus.DEAD_LETTER.value, 0)
        failed = counts.get(JobStatus.FAILED.value, 0)
        return JobHealth(
            queued=counts.get(JobStatus.QUEUED.value, 0),
            running=counts.get(JobStatus.RUNNING.value, 0),
            failed=failed,
            dead_letter=dead_letter,
            healthy=dead_letter == 0 and failed == 0,
        )

    def log(self, job_id: str, event_type: str, message: str, details: Mapping[str, Any]) -> None:
        self.db.execute(
            """
            INSERT INTO background_job_logs (job_id, event_type, message, details_json, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                job_id,
                event_type,
                message,
                json.dumps(dict(details), default=str, sort_keys=True),
                datetime.now(timezone.utc).isoformat(),
            ),
        )

