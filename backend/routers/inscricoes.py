"""CRUD endpoints for PGFN inscriptions."""

import uuid
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..models import Inscricao, Company, InscricaoCreate, InscricaoResponse

router = APIRouter(prefix="/api/inscricoes", tags=["inscricoes"])


@router.get("/{company_id}", response_model=List[InscricaoResponse])
async def list_inscricoes(
    company_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Inscricao)
        .where(Inscricao.company_id == company_id)
        .order_by(Inscricao.data_inscricao.desc().nullslast())
    )
    return result.scalars().all()


@router.post("", response_model=InscricaoResponse, status_code=201)
async def create_inscricao(
    data: InscricaoCreate,
    db: AsyncSession = Depends(get_db),
):
    # Dedup by numero_inscricao
    if data.numero_inscricao:
        existing = await db.execute(
            select(Inscricao).where(Inscricao.numero_inscricao == data.numero_inscricao)
        )
        if existing.scalar_one_or_none():
            raise HTTPException(400, f"Inscrição {data.numero_inscricao} already exists")

    obj = Inscricao(**data.model_dump())
    db.add(obj)
    await db.flush()

    # Update company aggregates
    await _update_company_aggregates(db, data.company_id)

    await db.refresh(obj)
    return obj


@router.post("/import/{company_id}", response_model=dict)
async def import_inscricoes(
    company_id: uuid.UUID,
    inscricoes: List[InscricaoCreate],
    db: AsyncSession = Depends(get_db),
):
    # Verify company exists
    company = await db.execute(select(Company).where(Company.id == company_id))
    if not company.scalar_one_or_none():
        raise HTTPException(404, "Company not found")

    imported = 0
    duplicates = 0
    for item in inscricoes:
        item.company_id = company_id
        if item.numero_inscricao:
            existing = await db.execute(
                select(Inscricao).where(Inscricao.numero_inscricao == item.numero_inscricao)
            )
            if existing.scalar_one_or_none():
                duplicates += 1
                continue
        obj = Inscricao(**item.model_dump())
        db.add(obj)
        imported += 1

    await db.flush()
    await _update_company_aggregates(db, company_id)

    return {"importadas": imported, "duplicatas": duplicates}


async def _update_company_aggregates(db: AsyncSession, company_id: uuid.UUID):
    """Update qtd_inscricoes and valor_aberto on parent company."""
    count_q = await db.execute(
        select(func.count(Inscricao.id)).where(Inscricao.company_id == company_id)
    )
    sum_q = await db.execute(
        select(func.coalesce(func.sum(Inscricao.valor_consolidado), 0))
        .where(Inscricao.company_id == company_id)
    )
    company_q = await db.execute(select(Company).where(Company.id == company_id))
    company = company_q.scalar_one_or_none()
    if company:
        company.qtd_inscricoes = count_q.scalar() or 0
        company.valor_aberto = sum_q.scalar() or 0
