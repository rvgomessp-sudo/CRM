"""VF CRM v2.0 — FastAPI Application."""

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .database import init_db
from .routers import (
    companies, interactions, pipeline, settings as settings_router,
    inscricoes, consultas, documentos, propostas, auth_router,
)

# ─── Sentry (optional) ────────────────────────────────────────────────────────
SENTRY_DSN = os.getenv("SENTRY_DSN", "")
if SENTRY_DSN:
    try:
        import sentry_sdk
        from sentry_sdk.integrations.fastapi import FastApiIntegration
        sentry_sdk.init(
            dsn=SENTRY_DSN,
            traces_sample_rate=0.1,
            environment=os.getenv("ENVIRONMENT", "production"),
            integrations=[FastApiIntegration()],
        )
    except ImportError:
        pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    # In dev (SQLite, no Alembic), create tables on startup.
    # In production, Alembic handles schema via the Dockerfile CMD.
    if os.getenv("SKIP_INIT_DB", "false").lower() not in ("true", "1", "yes"):
        await init_db()

    # Auto-backfill: corrige empresas com frente=NULL ou seguradora=NULL
    # (acontece quando importação foi feita antes da correção que default frente=1)
    try:
        from sqlalchemy import update, select, func
        from .database import async_session
        from .models import Company
        async with async_session() as session:
            # Count companies needing backfill
            r = await session.execute(
                select(func.count(Company.id)).where(Company.frente.is_(None))
            )
            null_count = r.scalar() or 0
            if null_count > 0:
                print(f"[startup] Backfill: corrigindo {null_count} empresas com frente=NULL")
                await session.execute(
                    update(Company).where(Company.frente.is_(None)).values(frente=1)
                )
                await session.execute(
                    update(Company).where(Company.seguradora_elegivel.is_(None)).values(seguradora_elegivel="Sancor")
                )
                await session.commit()
                print(f"[startup] Backfill concluído")
    except Exception as e:
        print(f"[startup] Backfill falhou (não crítico): {e}")

    yield


app = FastAPI(
    title="VF CRM v2.0",
    description="CRM proprietário Grupo V&F — Esteira Sancor PGFN",
    version="2.0.0",
    lifespan=lifespan,
)

# CORS: read allowed origins from env (comma-separated). Default = wildcard for local dev.
cors_origins_env = os.getenv("CORS_ORIGINS", "*")
allow_origins = ["*"] if cors_origins_env == "*" else [o.strip() for o in cors_origins_env.split(",")]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(auth_router.router)
app.include_router(companies.router)
app.include_router(interactions.router)
app.include_router(pipeline.router)
app.include_router(settings_router.router)
app.include_router(inscricoes.router)
app.include_router(consultas.router)
app.include_router(documentos.router)
app.include_router(propostas.router)


@app.get("/api/health")
async def health_check():
    return {
        "status": "ok",
        "version": "2.0.0",
        "auth_required": True,
        "environment": os.getenv("ENVIRONMENT", "development"),
    }
