"""SQLAlchemy ORM models and Pydantic schemas for VF CRM v2."""

import uuid
from datetime import datetime, date
from decimal import Decimal
from typing import Optional, List

from pydantic import BaseModel, Field
from sqlalchemy import (
    Column, String, Integer, Boolean, DateTime, Date, Numeric, Text,
    ForeignKey, func
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from .database import Base


# ─── SQLAlchemy ORM Models ───────────────────────────────────────────────────

class Company(Base):
    __tablename__ = "companies"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    cnpj = Column(String(18), unique=True, nullable=False)
    razao_social = Column(String(255))
    uf = Column(String(2))
    score_vf = Column(Numeric(5, 2))
    prioridade = Column(String(20))
    faixa_valor = Column(String(30))
    valor_aberto = Column(Numeric(15, 2))
    qtd_inscricoes = Column(Integer)
    anos_pgfn = Column(Numeric(4, 1))
    ano_ult_inscricao = Column(Integer)
    situacao_processual = Column(String(100))
    receita_principal = Column(String(100))
    simples_nacional = Column(Boolean, default=False)
    frente = Column(Integer)  # 1 or 2
    seguradora_elegivel = Column(String(50))
    responsavel = Column(String(20))  # Anna, Rodrigo, Ambos
    estagio_pipeline = Column(String(50))
    data_entrada_estagio = Column(DateTime)
    proximo_followup = Column(Date)
    score_confirmado = Column(Numeric(5, 2))
    limite_aprovado = Column(Numeric(15, 2))
    taxa_minima = Column(Numeric(5, 4))
    pl = Column(Numeric(15, 2))
    faturamento = Column(Numeric(15, 2))
    ebitda = Column(Numeric(15, 2))
    regime_tributario = Column(String(50))
    porte = Column(String(30))
    capital_social = Column(Numeric(15, 2))
    data_balanco = Column(Date)
    decisor_nome = Column(String(100))
    decisor_cargo = Column(String(100))
    decisor_linkedin = Column(String(255))
    decisor_email = Column(String(100))
    decisor_email_confirmado = Column(Boolean, default=False)
    decisor_telefone = Column(String(30))
    valor_garantia = Column(Numeric(15, 2))
    prazo_anos = Column(Integer)
    taxa_negociada = Column(Numeric(5, 4))
    premio_calculado = Column(Numeric(15, 2))
    honorarios = Column(Numeric(15, 2))
    receita_vf = Column(Numeric(15, 2))
    status_proposta = Column(String(50))
    data_proposta = Column(Date)
    numero_apolice = Column(String(50))
    data_emissao = Column(Date)
    comissao_recebida = Column(Boolean, default=False)
    honorarios_recebidos = Column(Boolean, default=False)
    status = Column(String(20), default="active")
    enrichment_status = Column(String(30), default="pgfn_only")
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    created_by = Column(String(20))
    updated_by = Column(String(20))

    interactions = relationship("Interaction", back_populates="company", lazy="selectin")


class Interaction(Base):
    __tablename__ = "interactions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_id = Column(UUID(as_uuid=True), ForeignKey("companies.id"), nullable=False)
    data_hora = Column(DateTime, default=func.now())
    responsavel = Column(String(20))
    canal = Column(String(30))
    resumo = Column(Text)
    proxima_acao = Column(Text)
    estagio_anterior = Column(String(50))
    estagio_novo = Column(String(50))

    company = relationship("Company", back_populates="interactions")


class Setting(Base):
    __tablename__ = "settings"

    key = Column(String(100), primary_key=True)
    value = Column(Text)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    updated_by = Column(String(20))


# ─── Pydantic Schemas ────────────────────────────────────────────────────────

class CompanyBase(BaseModel):
    cnpj: str
    razao_social: Optional[str] = None
    uf: Optional[str] = None
    score_vf: Optional[Decimal] = None
    prioridade: Optional[str] = None
    faixa_valor: Optional[str] = None
    valor_aberto: Optional[Decimal] = None
    qtd_inscricoes: Optional[int] = None
    anos_pgfn: Optional[Decimal] = None
    ano_ult_inscricao: Optional[int] = None
    situacao_processual: Optional[str] = None
    receita_principal: Optional[str] = None
    simples_nacional: Optional[bool] = False
    frente: Optional[int] = None
    seguradora_elegivel: Optional[str] = None
    responsavel: Optional[str] = None
    estagio_pipeline: Optional[str] = None
    proximo_followup: Optional[date] = None
    score_confirmado: Optional[Decimal] = None
    limite_aprovado: Optional[Decimal] = None
    taxa_minima: Optional[Decimal] = None
    pl: Optional[Decimal] = None
    faturamento: Optional[Decimal] = None
    ebitda: Optional[Decimal] = None
    regime_tributario: Optional[str] = None
    porte: Optional[str] = None
    capital_social: Optional[Decimal] = None
    data_balanco: Optional[date] = None
    decisor_nome: Optional[str] = None
    decisor_cargo: Optional[str] = None
    decisor_linkedin: Optional[str] = None
    decisor_email: Optional[str] = None
    decisor_email_confirmado: Optional[bool] = False
    decisor_telefone: Optional[str] = None
    valor_garantia: Optional[Decimal] = None
    prazo_anos: Optional[int] = None
    taxa_negociada: Optional[Decimal] = None
    premio_calculado: Optional[Decimal] = None
    honorarios: Optional[Decimal] = None
    receita_vf: Optional[Decimal] = None
    status_proposta: Optional[str] = None
    data_proposta: Optional[date] = None
    numero_apolice: Optional[str] = None
    data_emissao: Optional[date] = None
    comissao_recebida: Optional[bool] = False
    honorarios_recebidos: Optional[bool] = False
    enrichment_status: Optional[str] = "pgfn_only"
    created_by: Optional[str] = None
    updated_by: Optional[str] = None


class CompanyCreate(CompanyBase):
    pass


class CompanyUpdate(BaseModel):
    razao_social: Optional[str] = None
    uf: Optional[str] = None
    score_vf: Optional[Decimal] = None
    prioridade: Optional[str] = None
    faixa_valor: Optional[str] = None
    valor_aberto: Optional[Decimal] = None
    qtd_inscricoes: Optional[int] = None
    anos_pgfn: Optional[Decimal] = None
    ano_ult_inscricao: Optional[int] = None
    situacao_processual: Optional[str] = None
    receita_principal: Optional[str] = None
    simples_nacional: Optional[bool] = None
    frente: Optional[int] = None
    seguradora_elegivel: Optional[str] = None
    responsavel: Optional[str] = None
    estagio_pipeline: Optional[str] = None
    proximo_followup: Optional[date] = None
    score_confirmado: Optional[Decimal] = None
    limite_aprovado: Optional[Decimal] = None
    taxa_minima: Optional[Decimal] = None
    pl: Optional[Decimal] = None
    faturamento: Optional[Decimal] = None
    ebitda: Optional[Decimal] = None
    regime_tributario: Optional[str] = None
    porte: Optional[str] = None
    capital_social: Optional[Decimal] = None
    data_balanco: Optional[date] = None
    decisor_nome: Optional[str] = None
    decisor_cargo: Optional[str] = None
    decisor_linkedin: Optional[str] = None
    decisor_email: Optional[str] = None
    decisor_email_confirmado: Optional[bool] = None
    decisor_telefone: Optional[str] = None
    valor_garantia: Optional[Decimal] = None
    prazo_anos: Optional[int] = None
    taxa_negociada: Optional[Decimal] = None
    premio_calculado: Optional[Decimal] = None
    honorarios: Optional[Decimal] = None
    receita_vf: Optional[Decimal] = None
    status_proposta: Optional[str] = None
    data_proposta: Optional[date] = None
    numero_apolice: Optional[str] = None
    data_emissao: Optional[date] = None
    comissao_recebida: Optional[bool] = None
    honorarios_recebidos: Optional[bool] = None
    enrichment_status: Optional[str] = None
    updated_by: Optional[str] = None


class CompanyResponse(CompanyBase):
    id: uuid.UUID
    status: str = "active"
    data_entrada_estagio: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class InteractionBase(BaseModel):
    company_id: uuid.UUID
    responsavel: Optional[str] = None
    canal: Optional[str] = None
    resumo: Optional[str] = None
    proxima_acao: Optional[str] = None
    estagio_anterior: Optional[str] = None
    estagio_novo: Optional[str] = None


class InteractionCreate(InteractionBase):
    pass


class InteractionResponse(InteractionBase):
    id: uuid.UUID
    data_hora: Optional[datetime] = None

    model_config = {"from_attributes": True}


class SettingResponse(BaseModel):
    key: str
    value: Optional[str] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[str] = None

    model_config = {"from_attributes": True}


class SettingUpdate(BaseModel):
    value: str
    updated_by: Optional[str] = None


class DashboardResponse(BaseModel):
    total_companies: int = 0
    total_active: int = 0
    followups_vencidos: int = 0
    empresas_triar: int = 0
    propostas_pendentes: int = 0
    receita_projetada: Decimal = Decimal("0")
    receita_realizada: Decimal = Decimal("0")
    pipeline_f1: dict = {}
    pipeline_f2: dict = {}
    por_seguradora: dict = {}
    por_rating: dict = {}


class PipelineResponse(BaseModel):
    frente: int
    estagios: dict = {}


class ImportResult(BaseModel):
    importadas: int = 0
    duplicatas: int = 0
    erros: int = 0
    detalhes_erros: List[str] = []
