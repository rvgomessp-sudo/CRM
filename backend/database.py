"""Database connection and session management."""

import os
from sqlalchemy import event
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from dotenv import load_dotenv

from .db_base import Base  # re-export for backward compat

load_dotenv()


def _get_database_url() -> str:
    url = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./vf_crm.db")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+asyncpg://", 1)
    elif url.startswith("postgresql://") and "asyncpg" not in url:
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


DATABASE_URL = _get_database_url()
_IS_SQLITE = DATABASE_URL.startswith("sqlite")

_engine_kwargs = {"echo": False}
if _IS_SQLITE:
    # SQLite: timeout em segundos para esperar lock liberar antes de erro
    _engine_kwargs["connect_args"] = {"timeout": 30}
else:
    _engine_kwargs["pool_size"] = 5
    _engine_kwargs["max_overflow"] = 10

engine = create_async_engine(DATABASE_URL, **_engine_kwargs)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


# SQLite: habilitar WAL mode e ajustar pragmas a cada conexão.
# WAL permite leituras concorrentes durante escritas (resolve "database is locked"
# quando frontend faz várias chamadas paralelas durante um import grande).
if _IS_SQLITE:
    @event.listens_for(engine.sync_engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


async def get_db():
    async with async_session() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def init_db():
    """Create all tables. Used in dev mode. In production, use Alembic migrations instead."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
