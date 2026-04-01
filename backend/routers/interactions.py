from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
import uuid

from backend.database import get_db
from backend.models import Interaction, Company

router = APIRouter(prefix="/api/interactions", tags=["interactions"])

class InteractionCreate(BaseModel):
    company_id: str
    responsavel: str
    canal: str
    resumo: str
    proxima_acao: Optional[str] = None
    estagio_anterior: Optional[str] = None
    estagio_novo: Optional[str] = None

@router.get("/{company_id}")
def list_interactions(company_id: str, db: Session = Depends(get_db)):
    items = db.query(Interaction).filter(
        Interaction.company_id == company_id
    ).order_by(Interaction.data_hora.desc()).all()
    return [
        {k: v for k, v in i.__dict__.items() if not k.startswith("_")}
        for i in items
    ]

@router.post("")
def create_interaction(data: InteractionCreate, db: Session = Depends(get_db)):
    company = db.query(Company).filter(Company.id == data.company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Empresa não encontrada")

    interaction = Interaction(
        id=str(uuid.uuid4()),
        **data.dict()
    )
    db.add(interaction)

    # Atualiza estágio se fornecido
    if data.estagio_novo and data.estagio_novo != company.estagio_pipeline:
        company.estagio_pipeline = data.estagio_novo
        from sqlalchemy.sql import func
        company.data_entrada_estagio = datetime.now()

    db.commit()
    return {k: v for k, v in interaction.__dict__.items() if not k.startswith("_")}
