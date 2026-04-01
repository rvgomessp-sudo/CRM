from sqlalchemy import Column, String, Boolean, Integer, Float, Date, DateTime, Text, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import uuid
from backend.database import Base

def gen_uuid():
    return str(uuid.uuid4())

class Company(Base):
    __tablename__ = "companies"

    id = Column(String, primary_key=True, default=gen_uuid)
    cnpj = Column(String(18), unique=True, nullable=False, index=True)
    razao_social = Column(String(255))
    uf = Column(String(2))

    # PGFN
    score_vf = Column(Float)
    prioridade = Column(String(20))
    faixa_valor = Column(String(30))
    valor_aberto = Column(Float)
    qtd_inscricoes = Column(Integer)
    anos_pgfn = Column(Float)
    ano_ult_inscricao = Column(Integer)
    situacao_processual = Column(String(100))
    receita_principal = Column(String(100))
    simples_nacional = Column(Boolean, default=False)

    # Pipeline
    frente = Column(Integer)  # 1 ou 2
    seguradora_elegivel = Column(String(50))
    responsavel = Column(String(20))  # Anna, Rodrigo, Ambos
    estagio_pipeline = Column(String(50), default="Base PGFN")
    data_entrada_estagio = Column(DateTime, default=func.now())
    proximo_followup = Column(Date)

    # Enriquecimento seguradora
    seguradora_consultada = Column(String(50))
    data_consulta_seguradora = Column(Date)
    limite_aprovado = Column(Float)
    taxa_minima = Column(Float)
    status_consulta = Column(String(30))  # Aprovado, Negado, Pendente, Em análise
    obs_underwriter = Column(Text)

    # Financeiro
    pl = Column(Float)
    faturamento = Column(Float)
    ebitda = Column(Float)
    regime_tributario = Column(String(50))
    porte = Column(String(30))
    capital_social = Column(Float)
    data_balanco = Column(Date)
    fonte_dados = Column(String(50))

    # Decisor
    decisor_nome = Column(String(100))
    decisor_cargo = Column(String(100))
    decisor_linkedin = Column(String(255))
    decisor_email = Column(String(100))
    decisor_email_confirmado = Column(Boolean, default=False)
    decisor_telefone = Column(String(30))

    # Proposta / Operação
    score_confirmado = Column(Float)
    valor_garantia = Column(Float)
    prazo_anos = Column(Integer)
    taxa_negociada = Column(Float)
    premio_calculado = Column(Float)
    honorarios = Column(Float)
    receita_vf = Column(Float)
    status_proposta = Column(String(50))
    data_proposta = Column(Date)
    numero_apolice = Column(String(50))
    data_emissao = Column(Date)
    comissao_recebida = Column(Boolean, default=False)
    honorarios_recebidos = Column(Boolean, default=False)

    # Meta
    enrichment_status = Column(String(30), default="pgfn_only")
    status = Column(String(20), default="active")
    observacoes = Column(Text)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    created_by = Column(String(20))
    updated_by = Column(String(20))

    interactions = relationship("Interaction", back_populates="company", cascade="all, delete-orphan")


class Interaction(Base):
    __tablename__ = "interactions"

    id = Column(String, primary_key=True, default=gen_uuid)
    company_id = Column(String, ForeignKey("companies.id"), nullable=False)
    data_hora = Column(DateTime, default=func.now())
    responsavel = Column(String(20))
    canal = Column(String(30))  # LinkedIn, Email, Telefone, Reunião, Interno
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
