from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from datetime import date, timedelta

from backend.database import get_db
from backend.models import Company, Interaction

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])

ESTAGIOS_F1 = [
    "Base PGFN", "Enriquecimento", "Abordagem", "Interesse Manifesto",
    "Análise Rápida", "Proposta Enviada", "Submetido Sancor",
    "Aprovado", "Fechado", "Receita Realizada"
]
ESTAGIOS_F2 = [
    "Qualificação", "Diagnóstico", "Engenharia de Balanço",
    "Narrativa/Dossiê", "Comitê", "Aprovado", "Fechado", "Receita Realizada"
]

META_F1 = {
    "triagem_semana": 15,
    "propostas_mes": 8,
    "operacoes_mes": 3,
    "receita_mes": 120000,
}
META_F2 = {
    "propostas_mes": 1,
    "operacoes_mes": 1,
    "receita_mes": 300000,
}

@router.get("")
def get_dashboard(db: Session = Depends(get_db)):
    today = date.today()
    inicio_mes = today.replace(day=1)
    inicio_semana = today - timedelta(days=today.weekday())

    active = db.query(Company).filter(Company.status == "active")

    # ── KPIs gerais ──────────────────────────────────────────────────────────
    total = active.count()
    total_f1 = active.filter(Company.frente == 1).count()
    total_f2 = active.filter(Company.frente == 2).count()

    # Follow-ups vencidos
    followups_vencidos = active.filter(
        Company.proximo_followup < today
    ).order_by(Company.proximo_followup).all()

    # A triar esta semana (F1: Score>=70, estágio Base PGFN, não Simples)
    a_triar = active.filter(
        Company.frente == 1,
        Company.score_vf >= 70,
        Company.estagio_pipeline == "Base PGFN",
        Company.simples_nacional == False
    ).count()

    # Análises rápidas pendentes (F1 aguardando Rodrigo)
    analises_pendentes = active.filter(
        Company.estagio_pipeline == "Interesse Manifesto",
        Company.frente == 1
    ).count()

    # Propostas pendentes de envio
    propostas_pendentes = active.filter(
        Company.estagio_pipeline == "Análise Rápida"
    ).count()

    # Receita projetada (pipeline ativo)
    receita_proj = db.query(func.sum(Company.receita_vf)).filter(
        Company.status == "active",
        Company.receita_vf.isnot(None)
    ).scalar() or 0

    # Receita realizada este mês
    receita_realizada = db.query(func.sum(Company.receita_vf)).filter(
        Company.status == "active",
        Company.estagio_pipeline == "Receita Realizada",
        Company.data_entrada_estagio >= inicio_mes
    ).scalar() or 0

    # ── Pipeline por estágio ──────────────────────────────────────────────────
    def pipeline_count(estagios, frente):
        result = {}
        for e in estagios:
            count = active.filter(
                Company.estagio_pipeline == e,
                Company.frente == frente
            ).count()
            result[e] = count
        return result

    pipeline_f1 = pipeline_count(ESTAGIOS_F1, 1)
    pipeline_f2 = pipeline_count(ESTAGIOS_F2, 2)

    # ── Operações F2 em andamento ──────────────────────────────────────────────
    f2_em_andamento = active.filter(
        Company.frente == 2,
        Company.estagio_pipeline.notin_(["Base PGFN", "Receita Realizada"])
    ).all()

    f2_list = []
    for c in f2_em_andamento:
        dias = (date.today() - c.data_entrada_estagio.date()).days if c.data_entrada_estagio else 0
        f2_list.append({
            "id": c.id,
            "razao_social": c.razao_social,
            "estagio": c.estagio_pipeline,
            "dias_estagio": dias,
            "alerta": dias > 14,
            "valor_garantia": c.valor_garantia,
            "responsavel": c.responsavel,
        })

    # ── Por seguradora ────────────────────────────────────────────────────────
    por_seguradora = {}
    for seg in ["Sancor", "Berkley", "Zurich/Swiss Re/Chubb"]:
        por_seguradora[seg] = active.filter(Company.seguradora_elegivel == seg).count()

    # ── Por rating ────────────────────────────────────────────────────────────
    def rating_label(score):
        if score is None: return "N/A"
        if score >= 90: return "A+"
        if score >= 80: return "A"
        if score >= 70: return "A-"
        if score >= 60: return "B+"
        if score >= 50: return "B"
        return "C"

    por_rating = {"A+": 0, "A": 0, "A-": 0, "B+": 0, "B": 0, "C": 0, "N/A": 0}
    for c in active.all():
        por_rating[rating_label(c.score_vf)] += 1

    return {
        "resumo": {
            "total_empresas": total,
            "total_f1": total_f1,
            "total_f2": total_f2,
            "followups_vencidos": len(followups_vencidos),
            "a_triar_semana": a_triar,
            "analises_pendentes": analises_pendentes,
            "propostas_pendentes": propostas_pendentes,
            "receita_projetada": receita_proj,
            "receita_realizada_mes": receita_realizada,
        },
        "metas": {"f1": META_F1, "f2": META_F2},
        "pipeline_f1": pipeline_f1,
        "pipeline_f2": pipeline_f2,
        "followups_vencidos": [
            {"id": c.id, "razao_social": c.razao_social, "data": str(c.proximo_followup),
             "estagio": c.estagio_pipeline, "responsavel": c.responsavel}
            for c in followups_vencidos[:20]
        ],
        "f2_em_andamento": f2_list,
        "por_seguradora": por_seguradora,
        "por_rating": por_rating,
    }
