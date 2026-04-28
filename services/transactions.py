from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

from .base import DatabaseConnection


@dataclass(frozen=True)
class TransactionManager:
    db: DatabaseConnection

    @contextmanager
    def boundary(self) -> Iterator[None]:
        try:
            self.db.execute("BEGIN")
            yield
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

