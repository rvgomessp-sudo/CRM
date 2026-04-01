"""Pipeline, dashboard, and follow-up endpoints."""

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import List

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func, case, and_
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..models import Company, Interaction, DashboardResponse, PipelineResponse, CompanyResponse

router = APIRouter(prefix="/api", tags=["pipeline"])

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

    return DashboardResponse(
        total_companies=total,
        total_active=active,
        followups_vencidos=followups,
        empresas_triar=triar,
        propostas_pendentes=propostas,
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
