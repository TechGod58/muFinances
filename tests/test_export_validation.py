from services.base import ServiceContext
from services.export_validation import ExportType, ExportValidationService


class FakeCursor:
    description = []

    def fetchall(self):
        return []


class FakeDb:
    def __init__(self):
        self.statements = []

    def execute(self, sql, parameters=()):
        self.statements.append((sql, parameters))
        return FakeCursor()

    def executemany(self, sql, parameters):
        self.statements.append((sql, list(parameters)))

    def commit(self):
        pass

    def rollback(self):
        pass


def test_pdf_artifact_validates_content_type_and_size():
    service = ExportValidationService(FakeDb())
    artifact = service.build_artifact(
        "pdf-1",
        ExportType.PDF,
        "board.pdf",
        "application/pdf",
        b"%PDF-1.4",
        {"page_count": 2},
    )

    result = service.validate(artifact)

    assert result.valid is True
    assert result.issues == ()


def test_wrong_content_type_is_error():
    service = ExportValidationService(FakeDb())
    artifact = service.build_artifact("xlsx-1", ExportType.EXCEL, "report.xlsx", "text/plain", b"data")

    result = service.validate(artifact)

    assert result.valid is False
    assert result.issues[0].code == "content_type"


def test_chart_without_spec_hash_warns_but_does_not_fail():
    service = ExportValidationService(FakeDb())
    artifact = service.build_artifact("chart-1", ExportType.CHART_PNG, "chart.png", "image/png", b"png")

    result = service.validate(artifact)

    assert result.valid is True
    assert result.issues[0].code == "chart_spec_missing"


def test_board_package_requires_pdf_and_excel():
    service = ExportValidationService(FakeDb())
    context = ServiceContext(user_id="admin", roles=("admin",))
    pdf = service.build_artifact("pdf-1", ExportType.PDF, "board.pdf", "application/pdf", b"pdf", {"page_count": 1})

    result = service.validate_board_package(context, "package-1", [pdf])

    assert result.valid is False
    assert any(issue.code == "missing_required_artifact" for issue in result.issues)


def test_validate_and_record_writes_artifact():
    db = FakeDb()
    service = ExportValidationService(db)
    context = ServiceContext(user_id="admin", roles=("admin",))
    artifact = service.build_artifact("api-1", ExportType.BI_API, "export.json", "application/json", b"{}")

    result = service.validate_and_record(context, artifact)

    assert result.valid is True
    assert any("insert into export_artifacts" in " ".join(sql.lower().split()) for sql, _ in db.statements)

