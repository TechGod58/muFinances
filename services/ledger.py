from __future__ import annotations

from typing import Any, Mapping

import hashlib
import json

from .audit import AuditService
from .audit_chain import AuditChainService
from .base import DatabaseConnection, ServiceContext, ValidationError, fetch_all, require_fields
from .idempotency import IdempotencyService
from .security import SecurityService
from .transactions import TransactionManager


class LedgerService:
    def __init__(
        self,
        db: DatabaseConnection,
        audit: AuditService | None = None,
        security: SecurityService | None = None,
        idempotency: IdempotencyService | None = None,
        audit_chain: AuditChainService | None = None,
        transactions: TransactionManager | None = None,
    ):
        self.db = db
        self.audit = audit or AuditService(db)
        self.security = security or SecurityService()
        self.idempotency = idempotency or IdempotencyService(db)
        self.audit_chain = audit_chain or AuditChainService(db)
        self.transactions = transactions or TransactionManager(db)

    def list_lines(self, context: ServiceContext, scenario_id: str | None = None) -> list[dict[str, Any]]:
        cursor = self.db.execute(
            """
            SELECT *
            FROM ledger_lines
            WHERE (? IS NULL OR scenario_id = ?)
            ORDER BY fiscal_period, department_code, account_code
            """,
            (scenario_id or context.scenario_id, scenario_id or context.scenario_id),
        )
        return fetch_all(cursor)

    def post_line(self, context: ServiceContext, payload: Mapping[str, Any]) -> str:
        if not self.security.can_post_ledger(context):
            self.security.require_role(context, "admin", "controller", "budget_office")

        require_fields(payload, ("scenario_id", "department_code", "account_code", "fiscal_period", "amount", "idempotency_key"))
        line_id = str(payload.get("id") or payload.get("line_id") or "")
        if not line_id:
            raise ValidationError("Ledger line id is required for idempotent posting")

        request_hash = hashlib.sha256(json.dumps(dict(payload), default=str, sort_keys=True).encode("utf-8")).hexdigest()
        with self.transactions.boundary():
            claim = self.idempotency.claim(str(payload["idempotency_key"]), "ledger.post_line", request_hash)
            if claim.status == "completed" and claim.response_ref:
                return claim.response_ref

            self.assert_mutable_line(line_id)
            self.db.execute(
                """
                INSERT OR IGNORE INTO ledger_lines (
                    id, scenario_id, department_code, account_code, fiscal_period, amount, source, version, posted_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    line_id,
                    payload["scenario_id"],
                    payload["department_code"],
                    payload["account_code"],
                    payload["fiscal_period"],
                    payload["amount"],
                    payload.get("source", "manual"),
                    payload.get("version", "working"),
                    context.user_id,
                ),
            )
            self.audit.record(context, "ledger.post_line", "ledger_line", line_id, payload)
            self.audit_chain.append("ledger_line", line_id, {"action": "post", "payload": dict(payload), "user_id": context.user_id})
            self.idempotency.complete(str(payload["idempotency_key"]), line_id, {"operation": "ledger.post_line"})
        return line_id

    def assert_mutable_line(self, line_id: str) -> None:
        cursor = self.db.execute(
            """
            SELECT immutable, status
            FROM ledger_lines
            WHERE id = ?
            """,
            (line_id,),
        )
        rows = fetch_all(cursor)
        if not rows:
            return
        row = rows[0]
        if bool(row.get("immutable")) or str(row.get("status", "")).lower() in {"posted", "locked", "closed"}:
            raise ValidationError("Posted or locked ledger lines are immutable")
