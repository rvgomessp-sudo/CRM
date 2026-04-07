"""Alembic env.py — supports both sync (PostgreSQL) and SQLite, reads DATABASE_URL."""

import os
import sys
from logging.config import fileConfig
from pathlib import Path

from sqlalchemy import engine_from_config, pool
from alembic import context
from dotenv import load_dotenv

# Make backend importable
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
load_dotenv()

# Import Base directly (no engine triggers) and models for autogenerate
from backend.db_base import Base  # noqa: E402
from backend import models  # noqa: F401, E402

config = context.config

# Override sqlalchemy.url with the one from .env
db_url = os.getenv("DATABASE_URL", "sqlite:///./vf_crm.db")
# For Alembic we want SYNC URLs, not async drivers
db_url = db_url.replace("postgresql+asyncpg://", "postgresql://")
db_url = db_url.replace("sqlite+aiosqlite://", "sqlite://")
config.set_main_option("sqlalchemy.url", db_url)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=url.startswith("sqlite"),
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=db_url.startswith("sqlite"),
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
