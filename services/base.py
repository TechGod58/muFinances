from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Protocol, Sequence


class DatabaseConnection(Protocol):
    def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] = ...) -> Any:
        ...

    def executemany(self, sql: str, parameters: Iterable[Sequence[Any] | Mapping[str, Any]]) -> Any:
        ...

    def commit(self) -> None:
        ...

    def rollback(self) -> None:
        ...


@dataclass(frozen=True)
class ServiceContext:
    user_id: str
    roles: tuple[str, ...] = ()
    scenario_id: str | None = None
    fiscal_period: str | None = None
    request_id: str | None = None


class ServiceError(RuntimeError):
    pass


class ValidationError(ServiceError):
    pass


class PermissionDenied(ServiceError):
    pass


def require_fields(payload: Mapping[str, Any], fields: Sequence[str]) -> None:
    missing = [field for field in fields if payload.get(field) in (None, "")]
    if missing:
        raise ValidationError(f"Missing required fields: {', '.join(missing)}")


def fetch_all(cursor: Any) -> list[dict[str, Any]]:
    columns = [column[0] for column in cursor.description or []]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

