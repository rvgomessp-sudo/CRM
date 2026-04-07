"""Pipeline, dashboard, and follow-up endpoints."""

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import List

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func, case, and_
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..models import Company, Interaction, Meta, MetaCreate, MetaUpdate, MetaResponse, DashboardResponse, PipelineResponse, CompanyResponse

from ..auth import require_auth

router = APIRouter(prefix="/api", tags=["pipeline"], dependencies=[Depends(require_auth)])

F1_STAGES = [
    "Base PGFN", "Enriquecimento", "Abordagem", "Interesse Manifesto",
    "Análise Rápida", "Proposta Enviada", "Submetido Sancor",
    "Aprovado", "Fechado", "Receita Realizada",
]

F2_STAGES = [
    "Qualificação", "Diagnóstico", "Engenharia de Balanço",
    "Narrativa/Dossiê", "Comitê", "Aprovado", "Fechado", "Receita Realizada",
]


@router.get("/dashboard", response_model=DashboardResponse)
async def get_dashboard(db: AsyncSession = Depends(get_db)):
    today = date.today()

    # Total counts
    total_q = await db.execute(select(func.count(Company.id)))
    total = total_q.scalar() or 0

    active_q = await db.execute(
        select(func.count(Company.id)).where(Company.status == "active")
    )
    active = active_q.scalar() or 0

    # Follow-ups vencidos
    followup_q = await db.execute(
        select(func.count(Company.id)).where(
            and_(
                Company.status == "active",
                Company.proximo_followup <= today,
                Company.proximo_followup.isnot(None),
            )
        )
    )
    followups = followup_q.scalar() or 0

    # Empresas a triar (Score >= 70, Base PGFN)
    triar_q = await db.execute(
        select(func.count(Company.id)).where(
            and_(
                Company.status == "active",
                Company.score_vf >= 70,
                Company.estagio_pipeline == "Base PGFN",
            )
        )
    )
    triar = triar_q.scalar() or 0

    # Propostas pendentes
    propostas_q = await db.execute(
        select(func.count(Company.id)).where(
            and_(
                Company.status == "active",
                Company.estagio_pipeline.in_(["Proposta Enviada", "Submetido Sancor"]),
            )
        )
    )
    propostas = propostas_q.scalar() or 0

    # Receita projetada (sum receita_vf for active pipeline stages)
    receita_proj_q = await db.execute(
        select(func.coalesce(func.sum(Company.receita_vf), 0)).where(
            and_(
                Company.status == "active",
                Company.estagio_pipeline.notin_(["Base PGFN", "Receita Realizada"]),
                Company.receita_vf.isnot(None),
            )
        )
    )
    receita_proj = receita_proj_q.scalar() or Decimal("0")

    # Receita realizada
    receita_real_q = await db.execute(
        select(func.coalesce(func.sum(Company.receita_vf), 0)).where(
            and_(
                Company.status == "active",
                Company.estagio_pipeline == "Receita Realizada",
                Company.receita_vf.isnot(None),
            )
        )
    )
    receita_real = receita_real_q.scalar() or Decimal("0")

    # Pipeline counts per front
    pipeline_f1 = {}
    pipeline_f2 = {}

    for stage in F1_STAGES:
        q = await db.execute(
            select(func.count(Company.id)).where(
                and_(Company.status == "active", Company.frente == 1, Company.estagio_pipeline == stage)
            )
        )
        pipeline_f1[stage] = q.scalar() or 0

    for stage in F2_STAGES:
        q = await db.execute(
            select(func.count(Company.id)).where(
                and_(Company.status == "active", Company.frente == 2, Company.estagio_pipeline == stage)
            )
        )
        pipeline_f2[stage] = q.scalar() or 0

    # By seguradora
    seg_q = await db.execute(
        select(Company.seguradora_elegivel, func.count(Company.id))
        .where(and_(Company.status == "active", Company.seguradora_elegivel.isnot(None)))
        .group_by(Company.seguradora_elegivel)
    )
    por_seguradora = {row[0]: row[1] for row in seg_q.all()}

    # By rating (based on score ranges)
    por_rating = {}
    rating_ranges = [
        ("A+", 90, 100), ("A", 80, 89.99), ("A-", 70, 79.99),
        ("B+", 60, 69.99), ("B", 0, 59.99),
    ]
    for label, low, high in rating_ranges:
        rq = await db.execute(
            select(func.count(Company.id)).where(
                and_(
                    Company.status == "active",
                    Company.score_vf >= Decimal(str(low)),
                    Company.score_vf <= Decimal(str(high)),
                )
            )
        )
        por_rating[label] = rq.scalar() or 0

    # Empresas enriquecidas (past Base PGFN)
    enriq_q = await db.execute(
        select(func.count(Company.id)).where(
            and_(
                Company.status == "active",
                Company.estagio_pipeline != "Base PGFN",
                Company.estagio_pipeline.isnot(None),
            )
        )
    )
    enriquecidas = enriq_q.scalar() or 0

    # Análises pendentes (Interesse Manifesto stage)
    analises_q = await db.execute(
        select(func.count(Company.id)).where(
            and_(Company.status == "active", Company.estagio_pipeline == "Interesse Manifesto")
        )
    )
    analises = analises_q.scalar() or 0

    # Casos aprovados
    aprovados_q = await db.execute(
        select(func.count(Company.id)).where(
            and_(Company.status == "active", Company.estagio_pipeline == "Aprovado")
        )
    )
    aprovados = aprovados_q.scalar() or 0

    # Casos emitidos (Fechado)
    emitidos_q = await db.execute(
        select(func.count(Company.id)).where(
            and_(Company.status == "active", Company.estagio_pipeline == "Fechado")
        )
    )
    emitidos = emitidos_q.scalar() or 0

    return DashboardResponse(
        total_companies=total,
        total_active=active,
        followups_vencidos=followups,
        empresas_triar=triar,
        empresas_enriquecidas=enriquecidas,
        analises_pendentes=analises,
        propostas_pendentes=propostas,
        casos_aprovados=aprovados,
        casos_emitidos=emitidos,
        receita_projetada=receita_proj,
        receita_realizada=receita_real,
        pipeline_f1=pipeline_f1,
        pipeline_f2=pipeline_f2,
        por_seguradora=por_seguradora,
        por_rating=por_rating,
    )


@router.get("/pipeline")
async def get_pipeline(
    frente: int = Query(None),
    db: AsyncSession = Depends(get_db),
):
    results = []
    fronts = [frente] if frente else [1, 2]

    for f in fronts:
        stages = F1_STAGES if f == 1 else F2_STAGES
        stage_counts = {}
        for stage in stages:
            q = await db.execute(
                select(func.count(Company.id)).where(
                    and_(Company.status == "active", Company.frente == f, Company.estagio_pipeline == stage)
                )
            )
            stage_counts[stage] = q.scalar() or 0
        results.append(PipelineResponse(frente=f, estagios=stage_counts))

    return results


@router.get("/followups", response_model=List[CompanyResponse])
async def get_followups(
    responsavel: str = Query(None),
    db: AsyncSession = Depends(get_db),
):
    q = select(Company).where(
        and_(
            Company.status == "active",
            Company.proximo_followup.isnot(None),
            Company.proximo_followup <= date.today(),
        )
    )
    if responsavel:
        q = q.where(Company.responsavel == responsavel)

    q = q.order_by(Company.proximo_followup.asc())
    result = await db.execute(q)
    return result.scalars().all()


TERMINAL_STAGES = {"Fechado", "Receita Realizada"}


@router.get("/pipeline/stale", response_model=List[CompanyResponse])
async def get_stale_companies(
    days: int = Query(7, description="Days threshold for stale alert"),
    frente: int = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Return companies stuck in a non-terminal stage for more than N days."""
    threshold = datetime.utcnow() - timedelta(days=days)
    q = select(Company).where(
        and_(
            Company.status == "active",
            Company.data_entrada_estagio.isnot(None),
            Company.data_entrada_estagio <= threshold,
            Company.estagio_pipeline.notin_(list(TERMINAL_STAGES)),
        )
    )
    if frente:
        q = q.where(Company.frente == frente)

    q = q.order_by(Company.data_entrada_estagio.asc())
    result = await db.execute(q)
    return result.scalars().all()


@router.get("/metas", response_model=List[MetaResponse])
async def list_metas(
    responsavel: str = Query(None),
    periodo: str = Query(None),
    db: AsyncSession = Depends(get_db),
):
    q = select(Meta)
    if responsavel:
        q = q.where(Meta.responsavel == responsavel)
    if periodo:
        q = q.where(Meta.periodo == periodo)
    q = q.order_by(Meta.periodo.desc(), Meta.tipo)
    result = await db.execute(q)
    return result.scalars().all()


@router.post("/metas", response_model=MetaResponse, status_code=201)
async def create_or_update_meta(
    data: MetaCreate,
    db: AsyncSession = Depends(get_db),
):
    # Upsert: check if meta already exists for this user/period/type
    existing = await db.execute(
        select(Meta).where(
            and_(
                Meta.responsavel == data.responsavel,
                Meta.periodo == data.periodo,
                Meta.tipo == data.tipo,
            )
        )
    )
    meta = existing.scalar_one_or_none()
    if meta:
        meta.meta_valor = data.meta_valor
        if data.realizado_valor is not None:
            meta.realizado_valor = data.realizado_valor
    else:
        meta = Meta(**data.model_dump())
        db.add(meta)

    await db.flush()
    await db.refresh(meta)
    return meta
