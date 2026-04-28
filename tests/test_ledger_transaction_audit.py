import pytest

from services.audit_chain import AuditChainService
from services.base import ServiceContext, ValidationError
from services.idempotency import IdempotencyService
from services.ledger import LedgerService


class FakeCursor:
    def __init__(self, rows=None, columns=None):
        self._rows = rows or []
        self.description = [(column,) for column in (columns or [])]

    def fetchall(self):
        return self._rows


class FakeDb:
    def __init__(self):
        self.idempotency = {}
        self.ledger = {}
        self.chain = []
        self.statements = []
        self.commits = 0
        self.rollbacks = 0

    def execute(self, sql, parameters=()):
        self.statements.append((sql, parameters))
        normalized = " ".join(sql.lower().split())
        if normalized == "begin":
            return FakeCursor()
        if "from idempotency_keys" in normalized:
            row = self.idempotency.get(parameters[0])
            if not row:
                return FakeCursor([], ["key", "status", "response_ref", "request_hash"])
            return FakeCursor(
                [(row["key"], row["status"], row.get("response_ref"), row["request_hash"])],
                ["key", "status", "response_ref", "request_hash"],
            )
        if normalized.startswith("insert into idempotency_keys"):
            self.idempotency[parameters[0]] = {
                "key": parameters[0],
                "operation": parameters[1],
                "request_hash": parameters[2],
                "status": "claimed",
            }
        if normalized.startswith("update idempotency_keys"):
            self.idempotency[parameters[2]]["status"] = "completed"
            self.idempotency[parameters[2]]["response_ref"] = parameters[0]
        if "from ledger_lines" in normalized and "select immutable" in normalized:
            row = self.ledger.get(parameters[0])
            if not row:
                return FakeCursor([], ["immutable", "status"])
            return FakeCursor([(row.get("immutable", False), row.get("status", "working"))], ["immutable", "status"])
        if normalized.startswith("insert or ignore into ledger_lines"):
            self.ledger[parameters[0]] = {"status": "working", "immutable": False}
        if "coalesce(max(sequence)" in normalized:
            return FakeCursor([(len(self.chain) + 1,)], ["next_sequence"])
        if "from audit_chain" in normalized and "record_hash" in normalized:
            if not self.chain:
                return FakeCursor([], ["record_hash"])
            return FakeCursor([(self.chain[-1]["record_hash"],)], ["record_hash"])
        if normalized.startswith("insert into audit_chain"):
            self.chain.append(
                {
                    "entity_type": parameters[0],
                    "entity_id": parameters[1],
                    "sequence": parameters[2],
                    "previous_hash": parameters[3],
                    "record_hash": parameters[4],
                    "payload_json": parameters[5],
                }
            )
        return FakeCursor()

    def executemany(self, sql, parameters):
        self.statements.append((sql, list(parameters)))

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_idempotency_rejects_reused_key_with_different_request():
    db = FakeDb()
    service = IdempotencyService(db)
    service.claim("abc", "ledger.post_line", "hash-one")

    with pytest.raises(ValidationError):
        service.claim("abc", "ledger.post_line", "hash-two")


def test_ledger_post_uses_transaction_and_audit_chain():
    db = FakeDb()
    service = LedgerService(db)
    context = ServiceContext(user_id="admin", roles=("admin",))

    line_id = service.post_line(
        context,
        {
            "id": "line-1",
            "scenario_id": "scenario",
            "department_code": "OPS",
            "account_code": "SALARY",
            "fiscal_period": "2026-01",
            "amount": 100,
            "idempotency_key": "post-line-1",
        },
    )

    assert line_id == "line-1"
    assert db.commits == 1
    assert db.rollbacks == 0
    assert len(db.chain) == 1


def test_immutable_ledger_line_cannot_be_reposted():
    db = FakeDb()
    db.ledger["line-1"] = {"status": "posted", "immutable": True}
    service = LedgerService(db)

    with pytest.raises(ValidationError):
        service.assert_mutable_line("line-1")


def test_audit_chain_detects_tampering():
    db = FakeDb()
    chain = AuditChainService(db)
    chain.append("ledger_line", "line-1", {"amount": 100})
    records = list(db.chain)
    chain.verify(records)

    records[0]["payload_json"] = '{"amount":200}'
    with pytest.raises(ValidationError):
        chain.verify(records)

