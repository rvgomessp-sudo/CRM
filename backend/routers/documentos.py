"""CRUD endpoints for document tracking."""

import uuid
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..models import Documento, DocumentoCreate, DocumentoUpdate, DocumentoResponse

from ..auth import require_auth

router = APIRouter(prefix="/api/documentos", tags=["documentos"], dependencies=[Depends(require_auth)])

VALID_TIPOS = {
    "balanco_dre", "balancete", "contrato_social", "ficha_judicial",
    "ir_socios", "covenants", "minuta", "ccg", "carta_nomeacao", "nda",
}

VALID_STATUSES = {"pendente", "recebido", "enviado", "validado"}

# Documents required before submission to Sancor
BLOCKING_DOCS_SUBMISSAO = {"balanco_dre", "contrato_social", "ficha_judicial"}


@router.get("/{company_id}", response_model=List[DocumentoResponse])
async def list_documentos(
    company_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Documento)
        .where(Documento.company_id == company_id)
        .order_by(Documento.created_at.desc())
    )
    return result.scalars().all()


@router.post("", response_model=DocumentoResponse, status_code=201)
async def create_documento(
    data: DocumentoCreate,
    db: AsyncSession = Depends(get_db),
):
    if data.tipo not in VALID_TIPOS:
        raise HTTPException(400, f"Tipo inválido. Valores aceitos: {', '.join(sorted(VALID_TIPOS))}")
    if data.status not in VALID_STATUSES:
        raise HTTPException(400, f"Status inválido. Valores aceitos: {', '.join(sorted(VALID_STATUSES))}")

    obj = Documento(**data.model_dump())
    db.add(obj)
    await db.flush()
    await db.refresh(obj)
    return obj


@router.put("/{documento_id}", response_model=DocumentoResponse)
async def update_documento(
    documento_id: uuid.UUID,
    update: DocumentoUpdate,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Documento).where(Documento.id == documento_id))
    doc = result.scalar_one_or_none()
    if not doc:
        raise HTTPException(404, "Documento not found")

    data = update.model_dump(exclude_unset=True)
    if "status" in data and data["status"] not in VALID_STATUSES:
        raise HTTPException(400, f"Status inválido. Valores aceitos: {', '.join(sorted(VALID_STATUSES))}")

    for key, value in data.items():
        setattr(doc, key, value)

    await db.flush()
    await db.refresh(doc)
    return doc


@router.get("/{company_id}/blocking")
async def check_blocking_docs(
    company_id: uuid.UUID,
    target_stage: str = Query("Submetido Sancor"),
    db: AsyncSession = Depends(get_db),
):
    """Return list of missing/insufficient documents that block advancement to target stage."""
    blocking = []

    if target_stage in ("Submetido Sancor", "Aprovado", "Fechado"):
        required_status = "recebido" if target_stage == "Submetido Sancor" else "enviado"

        for doc_tipo in BLOCKING_DOCS_SUBMISSAO:
            result = await db.execute(
                select(Documento).where(
                    Documento.company_id == company_id,
                    Documento.tipo == doc_tipo,
                    Documento.status.in_(
                        [s for s in VALID_STATUSES
                         if list(VALID_STATUSES).index(s) >= list(VALID_STATUSES).index(required_status)]
                    ),
                )
            )
            if not result.scalars().first():
                labels = {
                    "balanco_dre": "Balanço e DRE (3 últimos exercícios)",
                    "contrato_social": "Contrato Social / Estatuto",
                    "ficha_judicial": "Ficha Judicial",
                }
                blocking.append({
                    "tipo": doc_tipo,
                    "label": labels.get(doc_tipo, doc_tipo),
                    "required_status": required_status,
                    "current_status": "ausente",
                })

    return {"target_stage": target_stage, "blocking": blocking, "can_advance": len(blocking) == 0}
