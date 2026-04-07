"""Settings and API enrichment placeholder endpoints."""

import os
from datetime import datetime
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..models import Setting, SettingResponse, SettingUpdate

from ..auth import require_auth

router = APIRouter(tags=["settings"], dependencies=[Depends(require_auth)])


# ─── Settings CRUD ───────────────────────────────────────────────────────────

@router.get("/api/settings", response_model=List[SettingResponse])
async def list_settings(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Setting).order_by(Setting.key))
    return result.scalars().all()


@router.put("/api/settings/{key}", response_model=SettingResponse)
async def update_setting(
    key: str,
    update: SettingUpdate,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Setting).where(Setting.key == key))
    setting = result.scalar_one_or_none()

    if setting:
        setting.value = update.value
        setting.updated_by = update.updated_by
    else:
        setting = Setting(key=key, value=update.value, updated_by=update.updated_by)
        db.add(setting)

    await db.flush()
    await db.refresh(setting)
    return setting


# ─── Enrichment Placeholders ─────────────────────────────────────────────────

def _check_api_key(key_name: str) -> dict:
    key = os.getenv(key_name, "")
    if not key:
        return {
            "status": "not_configured",
            "mock": True,
            "data": None,
            "message": f"API key '{key_name}' not configured. Set it in Settings.",
        }
    return None


@router.get("/api/enrich/cnpj/{cnpj}")
async def enrich_cnpj(cnpj: str):
    """BrasilAPI / CNPJ.ws enrichment placeholder."""
    not_configured = _check_api_key("CNPJWS_API_KEY")
    if not_configured:
        return not_configured

    # When configured, this would call the real API
    return {"status": "configured", "mock": False, "data": {}, "message": "API call would be made here"}


@router.get("/api/enrich/financeiro/{cnpj}")
async def enrich_financeiro(cnpj: str):
    """Serasa/Neoway financial enrichment placeholder."""
    not_configured = _check_api_key("SERASA_API_KEY")
    if not_configured:
        return not_configured

    return {"status": "configured", "mock": False, "data": {}, "message": "API call would be made here"}


@router.get("/api/enrich/decisor/{cnpj}")
async def enrich_decisor(cnpj: str):
    """LinkedIn/Apollo decision-maker enrichment placeholder."""
    not_configured = _check_api_key("APOLLO_API_KEY")
    if not_configured:
        return not_configured

    return {"status": "configured", "mock": False, "data": {}, "message": "API call would be made here"}


@router.get("/api/enrich/seguradora/{cnpj}/{seguradora}")
async def enrich_seguradora(cnpj: str, seguradora: str):
    """Insurance limit/rate query placeholder."""
    key_map = {
        "sancor": "SANCOR_API_URL",
        "berkley": "BERKLEY_API_URL",
        "zurich": "ZURICH_API_URL",
    }
    key_name = key_map.get(seguradora.lower(), f"{seguradora.upper()}_API_URL")
    not_configured = _check_api_key(key_name)
    if not_configured:
        return not_configured

    return {"status": "configured", "mock": False, "data": {}, "message": "API call would be made here"}
