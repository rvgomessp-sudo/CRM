"""Interaction log endpoints."""

import uuid
from datetime import datetime
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..models import (
    Company, Interaction,
    InteractionCreate, InteractionResponse,
)

router = APIRouter(prefix="/api/interactions", tags=["interactions"])


@router.get("/{company_id}", response_model=List[InteractionResponse])
async def list_interactions(
    company_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Interaction)
        .where(Interaction.company_id == company_id)
        .order_by(Interaction.data_hora.desc())
    )
    return result.scalars().all()


@router.post("", response_model=InteractionResponse, status_code=201)
async def create_interaction(
    interaction: InteractionCreate,
    db: AsyncSession = Depends(get_db),
):
    # Verify company exists
    company_result = await db.execute(
        select(Company).where(Company.id == interaction.company_id)
    )
    company = company_result.scalar_one_or_none()
    if not company:
        raise HTTPException(404, "Company not found")

    obj = Interaction(**interaction.model_dump())

    # If stage change is specified, update company
    if interaction.estagio_novo and interaction.estagio_novo != company.estagio_pipeline:
        obj.estagio_anterior = company.estagio_pipeline
        company.estagio_pipeline = interaction.estagio_novo
        company.data_entrada_estagio = datetime.utcnow()

    # Update follow-up if proxima_acao mentions a date
    if interaction.proxima_acao:
        company.updated_by = interaction.responsavel

    db.add(obj)
    await db.flush()
    await db.refresh(obj)
    return obj
