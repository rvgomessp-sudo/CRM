from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os

from backend.database import init_db
from backend.routers import companies, interactions, dashboard, settings, enrich

app = FastAPI(
    title="VF CRM API",
    description="CRM proprietário Vazquez & Fonseca — Seguro Garantia Tributário",
    version="2.0.0"
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
app.include_router(dashboard.router)
app.include_router(settings.router)
app.include_router(enrich.router)

@app.get("/api/health")
def health():
    return {"status": "ok", "version": "2.0.0", "sistema": "VF CRM"}

@app.on_event("startup")
def startup():
    init_db()

# Serve frontend estático
if os.path.exists("frontend"):
    app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
