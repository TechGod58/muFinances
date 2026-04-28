from services.base import ServiceContext
from services.import_pipeline import ImportMappingVersion, ImportPipelineService, MappingField


class FakeCursor:
    def __init__(self, rows=None, columns=None):
        self._rows = rows or []
        self.description = [(column,) for column in (columns or [])]

    def fetchall(self):
        return self._rows


class FakeDb:
    def __init__(self):
        self.statements = []
        self.rows = []
        self.commits = 0
        self.rollbacks = 0

    def execute(self, sql, parameters=()):
        self.statements.append((sql, parameters))
        normalized = " ".join(sql.lower().split())
        if normalized == "begin":
            return FakeCursor()
        if "from import_staged_rows" in normalized:
            return FakeCursor(
                [(item["batch_id"], item["row_number"], item["row_hash"], item["row_json"], item["status"]) for item in self.rows],
                ["batch_id", "row_number", "row_hash", "row_json", "status"],
            )
        if normalized.startswith("insert into import_staged_rows"):
            self.rows.append(
                {
                    "batch_id": parameters[0],
                    "row_number": parameters[1],
                    "row_hash": parameters[2],
                    "row_json": parameters[3],
                    "status": parameters[4] if len(parameters) > 4 else "accepted",
                }
            )
        return FakeCursor()

    def executemany(self, sql, parameters):
        self.statements.append((sql, list(parameters)))

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_stream_csv_rows_chunks_large_content():
    service = ImportPipelineService(FakeDb())
    content = "department,amount\nART,100\nSCI,200\nOPS,300\n"

    chunks = list(service.stream_csv_rows(content, chunk_size=2))

    assert len(chunks) == 2
    assert chunks[0][0]["department"] == "ART"


def test_validate_rows_accepts_and_rejects_required_fields():
    service = ImportPipelineService(FakeDb())
    mapping = ImportMappingVersion(
        "budget",
        1,
        "erp",
        (
            MappingField("department", "department_code", required=True),
            MappingField("amount", "amount", required=True),
        ),
    )

    accepted, issues = service.validate_rows(mapping, [{"department": "ART", "amount": "100"}, {"department": "", "amount": "200"}])

    assert accepted == [{"department_code": "ART", "amount": "100"}]
    assert len(issues) == 1
    assert issues[0].field == "department"


def test_persist_preview_stages_rows_and_rejections():
    db = FakeDb()
    service = ImportPipelineService(db)
    context = ServiceContext(user_id="admin", roles=("admin",))

    preview = service.persist_preview(context, "batch-1", [{"department_code": "ART", "amount": "100"}], [])

    assert preview.accepted_rows == 1
    assert preview.rejected_rows == 0
    assert db.commits == 1
    assert len(db.rows) == 1

