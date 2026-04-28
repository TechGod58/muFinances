from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, Sequence

from .base import DatabaseConnection, fetch_all


class ComponentStatus(str, Enum):
    OK = "ok"
    WARNING = "warning"
    BLOCKED = "blocked"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class DashboardComponent:
    name: str
    status: ComponentStatus
    detail: str
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProductionDashboard:
    generated_at: str
    overall_status: ComponentStatus
    components: tuple[DashboardComponent, ...]


class ProductionDashboardService:
    def __init__(self, db: DatabaseConnection):
        self.db = db

    def build(
        self,
        database_mode: str,
        auth_mode: str,
        components: Sequence[DashboardComponent] = (),
    ) -> ProductionDashboard:
        base = [
            DashboardComponent("Database mode", self._mode_status(database_mode), database_mode),
            DashboardComponent("Auth mode", self._auth_status(auth_mode), auth_mode),
        ]
        all_components = tuple(base + list(components))
        return ProductionDashboard(
            generated_at=datetime.now(timezone.utc).isoformat(),
            overall_status=self.overall_status(all_components),
            components=all_components,
        )

    def migration_status(self) -> DashboardComponent:
        rows = fetch_all(
            self.db.execute(
                """
                SELECT migration_id, applied_at
                FROM schema_migrations
                ORDER BY applied_at DESC
                LIMIT 1
                """
            )
        )
        if not rows:
            return DashboardComponent("Migration status", ComponentStatus.BLOCKED, "No migrations applied")
        return DashboardComponent("Migration status", ComponentStatus.OK, f"Latest: {rows[0]['migration_id']}", rows[0])

    def worker_status(self) -> DashboardComponent:
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
        dead = counts.get("dead_letter", 0)
        failed = counts.get("failed", 0)
        if dead or failed:
            return DashboardComponent("Worker status", ComponentStatus.BLOCKED, "Worker failures require review", counts)
        return DashboardComponent("Worker status", ComponentStatus.OK, "Worker queue healthy", counts)

    def backup_status(self) -> DashboardComponent:
        rows = fetch_all(
            self.db.execute(
                """
                SELECT backup_id, status, created_at
                FROM backup_manifests
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
        )
        if not rows:
            return DashboardComponent("Backup status", ComponentStatus.BLOCKED, "No verified backup found")
        row = rows[0]
        status = ComponentStatus.OK if str(row["status"]).lower() == "verified" else ComponentStatus.WARNING
        return DashboardComponent("Backup status", status, f"Latest: {row['status']}", row)

    def health_status(self, failures: Sequence[str] = ()) -> DashboardComponent:
        if failures:
            return DashboardComponent("Health checks", ComponentStatus.BLOCKED, "Health checks failing", {"failures": list(failures)})
        return DashboardComponent("Health checks", ComponentStatus.OK, "Health checks passing")

    def alert_status(self, alerts: Sequence[Mapping[str, Any]]) -> DashboardComponent:
        blocker_count = sum(1 for alert in alerts if str(alert.get("severity", "")).lower() in {"blocker", "critical", "error"})
        if blocker_count:
            return DashboardComponent("Alerts", ComponentStatus.BLOCKED, f"{blocker_count} blocker alerts", {"alerts": list(alerts)})
        if alerts:
            return DashboardComponent("Alerts", ComponentStatus.WARNING, f"{len(alerts)} open alerts", {"alerts": list(alerts)})
        return DashboardComponent("Alerts", ComponentStatus.OK, "No open alerts")

    def logs_status(self, recent_errors: Sequence[str]) -> DashboardComponent:
        if recent_errors:
            return DashboardComponent("Logs", ComponentStatus.WARNING, "Recent errors detected", {"errors": list(recent_errors)})
        return DashboardComponent("Logs", ComponentStatus.OK, "No recent errors")

    def overall_status(self, components: Sequence[DashboardComponent]) -> ComponentStatus:
        statuses = {component.status for component in components}
        if ComponentStatus.BLOCKED in statuses:
            return ComponentStatus.BLOCKED
        if ComponentStatus.WARNING in statuses:
            return ComponentStatus.WARNING
        if ComponentStatus.UNKNOWN in statuses:
            return ComponentStatus.UNKNOWN
        return ComponentStatus.OK

    def _mode_status(self, mode: str) -> ComponentStatus:
        return ComponentStatus.OK if mode.lower() in {"postgresql", "production-postgresql"} else ComponentStatus.WARNING

    def _auth_status(self, mode: str) -> ComponentStatus:
        return ComponentStatus.OK if mode.lower() in {"sso", "sso-ad", "production-sso"} else ComponentStatus.WARNING

