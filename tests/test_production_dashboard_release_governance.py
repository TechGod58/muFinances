from services.base import ServiceContext, ValidationError
from services.production_dashboard import ComponentStatus, DashboardComponent, ProductionDashboardService
from services.release_governance import OperationalSignoff, PromotionStatus, ReleaseGovernanceService


class FakeCursor:
    def __init__(self, rows=None, columns=None):
        self._rows = rows or []
        self.description = [(column,) for column in (columns or [])]

    def fetchall(self):
        return self._rows


class FakeDb:
    def __init__(self):
        self.statements = []

    def execute(self, sql, parameters=()):
        self.statements.append((sql, parameters))
        normalized = " ".join(sql.lower().split())
        if "from schema_migrations" in normalized:
            return FakeCursor([("0088_cutover", "2026-04-27")], ["migration_id", "applied_at"])
        if "from background_jobs" in normalized and "group by status" in normalized:
            return FakeCursor([("queued", 2)], ["status", "count"])
        if "from backup_manifests" in normalized:
            return FakeCursor([("backup-1", "verified", "2026-04-27")], ["backup_id", "status", "created_at"])
        return FakeCursor()

    def executemany(self, sql, parameters):
        self.statements.append((sql, list(parameters)))

    def commit(self):
        pass

    def rollback(self):
        pass


def test_dashboard_overall_status_uses_worst_component():
    service = ProductionDashboardService(FakeDb())

    dashboard = service.build(
        "sqlite",
        "local",
        [DashboardComponent("Backup", ComponentStatus.BLOCKED, "missing")],
    )

    assert dashboard.overall_status is ComponentStatus.BLOCKED


def test_dashboard_components_can_read_statuses():
    service = ProductionDashboardService(FakeDb())

    assert service.migration_status().status is ComponentStatus.OK
    assert service.worker_status().status is ComponentStatus.OK
    assert service.backup_status().status is ComponentStatus.OK


def test_promotion_is_ready_when_all_checks_pass():
    service = ReleaseGovernanceService(FakeDb())
    checklist = {name: True for name in service.REQUIRED_CHECKS}

    assert service.evaluate_checklist(checklist) is PromotionStatus.READY


def test_promotion_is_blocked_when_check_missing():
    service = ReleaseGovernanceService(FakeDb())

    assert service.evaluate_checklist({"release_notes": True}) is PromotionStatus.BLOCKED


def test_config_export_redacts_secrets():
    service = ReleaseGovernanceService(FakeDb())

    exported = service.export_config({"database_url": "postgres", "api_key": "secret-value"})

    assert "secret-value" not in exported
    assert "<redacted>" in exported


def test_rollback_plan_requires_owner_steps_and_trigger():
    service = ReleaseGovernanceService(FakeDb())
    context = ServiceContext(user_id="admin", roles=("admin",))

    try:
        service.record_rollback_plan(context, "1.0.0", {"trigger": "failed smoke"})
    except ValidationError:
        pass
    else:
        raise AssertionError("Expected rollback validation error")


def test_signoff_requires_all_roles():
    service = ReleaseGovernanceService(FakeDb())
    signoffs = [
        OperationalSignoff("admin", "u1", True),
        OperationalSignoff("controller", "u2", True),
        OperationalSignoff("integration_owner", "u3", True),
        OperationalSignoff("security_owner", "u4", True),
    ]

    assert service.signoff_complete(signoffs) is True

