"""SQLAlchemy ORM models and Pydantic schemas for VF CRM v2 — Fase 1."""

import uuid
from datetime import datetime, date
from decimal import Decimal
from typing import Optional, List

from pydantic import BaseModel, Field
from sqlalchemy import (
    Column, String, Integer, Boolean, DateTime, Date, Numeric, Text,
    ForeignKey, func, TypeDecorator
)
from sqlalchemy.orm import relationship

from .db_base import Base


class UUIDType(TypeDecorator):
    """Platform-agnostic UUID type. Uses String(36) for SQLite, native UUID for PG."""
    impl = String(36)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            return str(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            return uuid.UUID(value) if not isinstance(value, uuid.UUID) else value
        return value


# ═══════════════════════════════════════════════════════════════════════════════
# SQLAlchemy ORM Models
# ═══════════════════════════════════════════════════════════════════════════════

class Company(Base):
    __tablename__ = "companies"

    id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
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
    # Decisor
    decisor_nome = Column(String(100))
    decisor_cargo = Column(String(100))
    decisor_linkedin = Column(String(255))
    decisor_email = Column(String(100))
    decisor_email_confirmado = Column(Boolean, default=False)
    decisor_telefone = Column(String(30))
    decisor_confianca = Column(String(10))  # baixa, media, alta
    decisor_validado = Column(Boolean, default=False)
    # Proposta (legacy fields kept for backward compat, new propostas table is primary)
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
    # Status & enrichment
    status = Column(String(20), default="active")  # active|archived|perdido|congelado|sem_retorno
    enrichment_status = Column(String(30), default="pgfn_only")
    situacao_cadastral_receita = Column(String(50))
    fallback_berkley = Column(Boolean, default=False)
    # Timestamps
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    created_by = Column(String(20))
    updated_by = Column(String(20))

    interactions = relationship("Interaction", back_populates="company", lazy="selectin")
    inscricoes = relationship("Inscricao", back_populates="company", lazy="noload")
    consultas = relationship("ConsultaSeguradora", back_populates="company", lazy="noload")
    documentos = relationship("Documento", back_populates="company", lazy="noload")
    propostas = relationship("Proposta", back_populates="company", lazy="noload")


class Interaction(Base):
    __tablename__ = "interactions"

    id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    company_id = Column(UUIDType, ForeignKey("companies.id"), nullable=False)
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


class Inscricao(Base):
    """Individual PGFN inscription entries per company."""
    __tablename__ = "inscricoes"

    id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    company_id = Column(UUIDType, ForeignKey("companies.id"), nullable=False)
    numero_inscricao = Column(String(50), unique=True)
    cnpj = Column(String(18))
    cnpj_raiz = Column(String(10))
    nome_devedor = Column(String(255))
    uf_devedor = Column(String(2))
    receita_principal = Column(String(100))
    valor_consolidado = Column(Numeric(15, 2))
    data_inscricao = Column(Date)
    safra = Column(String(10))
    indicador_ajuizado = Column(String(5))
    tipo_situacao_inscricao = Column(String(100))
    situacao_inscricao = Column(String(100))
    tipo_garantia = Column(String(100))
    situacao_macro = Column(String(100))
    qtd_corresponsaveis = Column(Integer)
    qtd_solidarios = Column(Integer)
    tem_pf_corresponsavel = Column(Boolean)
    unidade_responsavel = Column(String(100))
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    company = relationship("Company", back_populates="inscricoes")


class ConsultaSeguradora(Base):
    """Structured insurer query results (Sancor/Berkley)."""
    __tablename__ = "consultas_seguradora"

    id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    company_id = Column(UUIDType, ForeignKey("companies.id"), nullable=False)
    data_consulta = Column(DateTime, default=func.now())
    seguradora = Column(String(50), default="Sancor")
    modalidade = Column(String(100))
    limite_aprovado = Column(Numeric(15, 2))
    limite_disponivel = Column(Numeric(15, 2))
    taxa = Column(Numeric(5, 4))
    limite_judicial_pgfn = Column(Numeric(15, 2))
    taxa_pct_anual = Column(Numeric(5, 2))
    status = Column(String(30))  # aprovado_automatico|aprovado_indicativo|depende_mesa|outro_corretor|fora_politica|tomador_bloqueado|parametro_incompativel|reprovado|pendente
    exige_mesa_credito = Column(Boolean, default=False)
    exige_carta_nomeacao = Column(Boolean, default=False)
    motivo_reprovacao = Column(Text)
    observacoes = Column(Text)
    responsavel = Column(String(20))
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    company = relationship("Company", back_populates="consultas")


class Documento(Base):
    """Document tracking per company."""
    __tablename__ = "documentos"

    id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    company_id = Column(UUIDType, ForeignKey("companies.id"), nullable=False)
    tipo = Column(String(50))  # balanco_dre|balancete|contrato_social|ficha_judicial|ir_socios|covenants|minuta|ccg|carta_nomeacao|nda
    status = Column(String(20), default="pendente")  # pendente|recebido|enviado|validado
    data_recebimento = Column(Date)
    data_envio = Column(Date)
    observacoes = Column(Text)
    responsavel = Column(String(20))
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    company = relationship("Company", back_populates="documentos")


class Proposta(Base):
    """Proposal/operation entity — separated from company."""
    __tablename__ = "propostas"

    id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    company_id = Column(UUIDType, ForeignKey("companies.id"), nullable=False)
    numero_proposta = Column(String(50))
    valor_garantia = Column(Numeric(15, 2))
    prazo_anos = Column(Integer)
    taxa = Column(Numeric(5, 4))
    premio_bruto = Column(Numeric(15, 2))
    premio_liquido = Column(Numeric(15, 2))
    comissao_pct = Column(Numeric(5, 2))
    comissao_valor = Column(Numeric(15, 2))
    honorarios = Column(Numeric(15, 2))
    receita_vf_total = Column(Numeric(15, 2))
    flag_aderencia = Column(String(20))  # aderente|atencao|fora_politica
    teto_percepcao = Column(Numeric(15, 2))
    relacao_hon_premio = Column(Numeric(5, 2))
    status_proposta = Column(String(50))  # rascunho|enviada|aceita|negociacao|recusada|cancelada
    numero_apolice = Column(String(50))
    data_emissao = Column(Date)
    comissao_recebida = Column(Boolean, default=False)
    honorarios_recebidos = Column(Boolean, default=False)
    nda_status = Column(String(30))
    created_by = Column(String(20))
    updated_by = Column(String(20))
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    company = relationship("Company", back_populates="propostas")


class User(Base):
    """System user (Anna, Rodrigo)."""
    __tablename__ = "users"

    id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    username = Column(String(50), unique=True, nullable=False)
    nome_completo = Column(String(100))
    email = Column(String(100))
    senha_hash = Column(String(255), nullable=False)
    papel = Column(String(20), default="user")  # admin, user
    ativo = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())


class Meta(Base):
    """Per-user targets vs actual."""
    __tablename__ = "metas"

    id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    responsavel = Column(String(20), nullable=False)
    periodo = Column(String(10), nullable=False)  # YYYY-MM
    tipo = Column(String(50), nullable=False)
    meta_valor = Column(Numeric(15, 2))
    realizado_valor = Column(Numeric(15, 2), default=0)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())


# ═══════════════════════════════════════════════════════════════════════════════
# Pydantic Schemas
# ═══════════════════════════════════════════════════════════════════════════════

# ─── Company ──────────────────────────────────────────────────────────────────

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
    decisor_confianca: Optional[str] = None
    decisor_validado: Optional[bool] = False
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
    situacao_cadastral_receita: Optional[str] = None
    fallback_berkley: Optional[bool] = False
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
    decisor_confianca: Optional[str] = None
    decisor_validado: Optional[bool] = None
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
    situacao_cadastral_receita: Optional[str] = None
    fallback_berkley: Optional[bool] = None
    updated_by: Optional[str] = None


class CompanyResponse(CompanyBase):
    id: uuid.UUID
    status: str = "active"
    data_entrada_estagio: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


# ─── Interaction ──────────────────────────────────────────────────────────────

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


# ─── Setting ──────────────────────────────────────────────────────────────────

class SettingResponse(BaseModel):
    key: str
    value: Optional[str] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[str] = None

    model_config = {"from_attributes": True}


class SettingUpdate(BaseModel):
    value: str
    updated_by: Optional[str] = None


# ─── Inscricao ────────────────────────────────────────────────────────────────

class InscricaoBase(BaseModel):
    company_id: uuid.UUID
    numero_inscricao: Optional[str] = None
    cnpj: Optional[str] = None
    cnpj_raiz: Optional[str] = None
    nome_devedor: Optional[str] = None
    uf_devedor: Optional[str] = None
    receita_principal: Optional[str] = None
    valor_consolidado: Optional[Decimal] = None
    data_inscricao: Optional[date] = None
    safra: Optional[str] = None
    indicador_ajuizado: Optional[str] = None
    tipo_situacao_inscricao: Optional[str] = None
    situacao_inscricao: Optional[str] = None
    tipo_garantia: Optional[str] = None
    situacao_macro: Optional[str] = None
    qtd_corresponsaveis: Optional[int] = None
    qtd_solidarios: Optional[int] = None
    tem_pf_corresponsavel: Optional[bool] = None
    unidade_responsavel: Optional[str] = None


class InscricaoCreate(InscricaoBase):
    pass


class InscricaoResponse(InscricaoBase):
    id: uuid.UUID
    created_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


# ─── ConsultaSeguradora ──────────────────────────────────────────────────────

class ConsultaBase(BaseModel):
    company_id: uuid.UUID
    seguradora: str = "Sancor"
    modalidade: Optional[str] = None
    limite_aprovado: Optional[Decimal] = None
    limite_disponivel: Optional[Decimal] = None
    taxa: Optional[Decimal] = None
    limite_judicial_pgfn: Optional[Decimal] = None
    taxa_pct_anual: Optional[Decimal] = None
    status: str = "pendente"
    exige_mesa_credito: Optional[bool] = False
    exige_carta_nomeacao: Optional[bool] = False
    motivo_reprovacao: Optional[str] = None
    observacoes: Optional[str] = None
    responsavel: Optional[str] = None


class ConsultaCreate(ConsultaBase):
    pass


class ConsultaUpdate(BaseModel):
    status: Optional[str] = None
    limite_aprovado: Optional[Decimal] = None
    limite_disponivel: Optional[Decimal] = None
    taxa: Optional[Decimal] = None
    limite_judicial_pgfn: Optional[Decimal] = None
    taxa_pct_anual: Optional[Decimal] = None
    exige_mesa_credito: Optional[bool] = None
    exige_carta_nomeacao: Optional[bool] = None
    motivo_reprovacao: Optional[str] = None
    observacoes: Optional[str] = None


class ConsultaResponse(ConsultaBase):
    id: uuid.UUID
    data_consulta: Optional[datetime] = None
    created_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


# ─── Documento ────────────────────────────────────────────────────────────────

class DocumentoBase(BaseModel):
    company_id: uuid.UUID
    tipo: str
    status: str = "pendente"
    data_recebimento: Optional[date] = None
    data_envio: Optional[date] = None
    observacoes: Optional[str] = None
    responsavel: Optional[str] = None


class DocumentoCreate(DocumentoBase):
    pass


class DocumentoUpdate(BaseModel):
    status: Optional[str] = None
    data_recebimento: Optional[date] = None
    data_envio: Optional[date] = None
    observacoes: Optional[str] = None
    responsavel: Optional[str] = None


class DocumentoResponse(DocumentoBase):
    id: uuid.UUID
    created_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


# ─── Proposta ─────────────────────────────────────────────────────────────────

class PropostaBase(BaseModel):
    company_id: uuid.UUID
    numero_proposta: Optional[str] = None
    valor_garantia: Optional[Decimal] = None
    prazo_anos: Optional[int] = None
    taxa: Optional[Decimal] = None
    comissao_pct: Optional[Decimal] = Field(default=Decimal("20"))
    honorarios: Optional[Decimal] = None
    teto_percepcao: Optional[Decimal] = None
    status_proposta: Optional[str] = "rascunho"
    nda_status: Optional[str] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None


class PropostaCreate(PropostaBase):
    pass


class PropostaUpdate(BaseModel):
    numero_proposta: Optional[str] = None
    valor_garantia: Optional[Decimal] = None
    prazo_anos: Optional[int] = None
    taxa: Optional[Decimal] = None
    comissao_pct: Optional[Decimal] = None
    honorarios: Optional[Decimal] = None
    teto_percepcao: Optional[Decimal] = None
    status_proposta: Optional[str] = None
    numero_apolice: Optional[str] = None
    data_emissao: Optional[date] = None
    comissao_recebida: Optional[bool] = None
    honorarios_recebidos: Optional[bool] = None
    nda_status: Optional[str] = None
    updated_by: Optional[str] = None


class PropostaResponse(PropostaBase):
    id: uuid.UUID
    premio_bruto: Optional[Decimal] = None
    premio_liquido: Optional[Decimal] = None
    comissao_valor: Optional[Decimal] = None
    receita_vf_total: Optional[Decimal] = None
    flag_aderencia: Optional[str] = None
    relacao_hon_premio: Optional[Decimal] = None
    numero_apolice: Optional[str] = None
    data_emissao: Optional[date] = None
    comissao_recebida: bool = False
    honorarios_recebidos: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


# ─── Meta ─────────────────────────────────────────────────────────────────────

class MetaBase(BaseModel):
    responsavel: str
    periodo: str
    tipo: str
    meta_valor: Optional[Decimal] = None
    realizado_valor: Optional[Decimal] = Decimal("0")


class MetaCreate(MetaBase):
    pass


class MetaUpdate(BaseModel):
    meta_valor: Optional[Decimal] = None
    realizado_valor: Optional[Decimal] = None


class MetaResponse(MetaBase):
    id: uuid.UUID
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


# ─── Dashboard & Pipeline ─────────────────────────────────────────────────────

class DashboardResponse(BaseModel):
    total_companies: int = 0
    total_active: int = 0
    followups_vencidos: int = 0
    empresas_triar: int = 0
    empresas_enriquecidas: int = 0
    analises_pendentes: int = 0
    propostas_pendentes: int = 0
    casos_aprovados: int = 0
    casos_emitidos: int = 0
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


class UserResponse(BaseModel):
    id: uuid.UUID
    username: str
    nome_completo: Optional[str] = None
    email: Optional[str] = None
    papel: str = "user"
    ativo: bool = True

    model_config = {"from_attributes": True}


class UserCreate(BaseModel):
    username: str
    nome_completo: Optional[str] = None
    email: Optional[str] = None
    senha: str
    papel: str = "user"


class LoginRequest(BaseModel):
    username: str
    senha: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserResponse


class SolverResult(BaseModel):
    taxa: Decimal
    premio_bruto: Decimal
    premio_liquido: Decimal
    comissao_valor: Decimal
    honorarios: Decimal
    receita_vf_total: Decimal
    custo_cliente: Decimal
    relacao_hon_premio: Decimal
    flag_aderencia: str
