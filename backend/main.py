"""VF CRM v2.0 — FastAPI Application."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .database import init_db
from .routers import companies, interactions, pipeline, settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(
    title="VF CRM v2.0",
    description="CRM proprietário V&F para gestão do pipeline de Seguro Garantia Tributário",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(companies.router)
app.include_router(interactions.router)
app.include_router(pipeline.router)
app.include_router(settings.router)


@app.get("/api/health")
async def health_check():
    return {"status": "ok", "version": "2.0.0"}
