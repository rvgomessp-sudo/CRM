"""Declarative Base — importable without triggering engine creation.
Used by both database.py (runtime) and alembic env.py (migrations)."""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass
