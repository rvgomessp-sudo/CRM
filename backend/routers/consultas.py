"""CRUD endpoints for structured insurer queries (Sancor/Berkley)."""

import uuid
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..models import (
    ConsultaSeguradora, Company,
    ConsultaCreate, ConsultaUpdate, ConsultaResponse,
)

router = APIRouter(prefix="/api/consultas", tags=["consultas"])

VALID_STATUSES = {
    "aprovado_automatico", "aprovado_indicativo", "depende_mesa",
    "outro_corretor", "fora_politica", "tomador_bloqueado",
    "parametro_incompativel", "reprovado", "pendente",
}

APPROVED_STATUSES = {"aprovado_automatico", "aprovado_indicativo"}
REJECTED_STATUSES = {"reprovado", "fora_politica", "tomador_bloqueado"}


@router.get("/{company_id}", response_model=List[ConsultaResponse])
async def list_consultas(
    company_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(ConsultaSeguradora)
        .where(ConsultaSeguradora.company_id == company_id)
        .order_by(ConsultaSeguradora.data_consulta.desc())
    )
    return result.scalars().all()


@router.post("", response_model=ConsultaResponse, status_code=201)
async def create_consulta(
    data: ConsultaCreate,
    db: AsyncSession = Depends(get_db),
):
    if data.status and data.status not in VALID_STATUSES:
        raise HTTPException(400, f"Status inválido. Valores aceitos: {', '.join(sorted(VALID_STATUSES))}")

    # Verify company
    company_q = await db.execute(select(Company).where(Company.id == data.company_id))
    company = company_q.scalar_one_or_none()
    if not company:
        raise HTTPException(404, "Company not found")

    obj = ConsultaSeguradora(**data.model_dump())
    db.add(obj)
    await db.flush()

    # Auto-update company based on result
    await _sync_company_from_consulta(db, company, data.status)

    await db.refresh(obj)
    return obj


@router.put("/{consulta_id}", response_model=ConsultaResponse)
async def update_consulta(
    consulta_id: uuid.UUID,
    update: ConsultaUpdate,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(ConsultaSeguradora).where(ConsultaSeguradora.id == consulta_id)
    )
    consulta = result.scalar_one_or_none()
    if not consulta:
        raise HTTPException(404, "Consulta not found")

    data = update.model_dump(exclude_unset=True)
    for key, value in data.items():
        setattr(consulta, key, value)

    await db.flush()

    # Sync company if status changed
    if "status" in data:
        company_q = await db.execute(select(Company).where(Company.id == consulta.company_id))
        company = company_q.scalar_one_or_none()
        if company:
            await _sync_company_from_consulta(db, company, data["status"])

    await db.refresh(consulta)
    return consulta


async def _sync_company_from_consulta(db: AsyncSession, company: Company, status: str):
    """Update company fields based on consulta result."""
    if status in APPROVED_STATUSES:
        # Get the latest approved consulta to sync limits
        latest = await db.execute(
            select(ConsultaSeguradora)
            .where(
                ConsultaSeguradora.company_id == company.id,
                ConsultaSeguradora.status.in_(APPROVED_STATUSES),
            )
            .order_by(ConsultaSeguradora.data_consulta.desc())
        )
        approved = latest.scalars().first()
        if approved:
            if approved.limite_aprovado:
                company.limite_aprovado = approved.limite_aprovado
            if approved.taxa:
                company.taxa_minima = approved.taxa

    elif status in REJECTED_STATUSES:
        # Check if there's any other approved consulta
        other_approved = await db.execute(
            select(ConsultaSeguradora)
            .where(
                ConsultaSeguradora.company_id == company.id,
                ConsultaSeguradora.status.in_(APPROVED_STATUSES),
            )
        )
        if not other_approved.scalars().first():
            company.fallback_berkley = True
