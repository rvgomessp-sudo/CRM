"""Stage transition validation engine for VF CRM Fase 1 pipeline."""

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Company, Interaction, Inscricao, ConsultaSeguradora, Documento, Proposta

STAGE_STATUS_ORDER = {
    "pendente": 0, "recebido": 1, "enviado": 2, "validado": 3,
}

BLOCKING_DOCS = ["balanco_dre", "contrato_social", "ficha_judicial"]


async def validate_transition(
    db: AsyncSession,
    company_id,
    from_stage: str,
    to_stage: str,
) -> tuple[bool, list[str]]:
    """Validate whether a company can move from one pipeline stage to another.
    Returns (valid, list_of_missing_requirements).
    """
    result = await db.execute(select(Company).where(Company.id == company_id))
    company = result.scalar_one_or_none()
    if not company:
        return False, ["Empresa não encontrada"]

    missing = []
    key = f"{from_stage} → {to_stage}"

    if to_stage == "Enriquecimento":
        if not company.razao_social:
            missing.append("Razão social")
        if not company.cnpj:
            missing.append("CNPJ")
        if not company.qtd_inscricoes or company.qtd_inscricoes <= 0:
            # Check inscricoes table
            insc_count = await db.execute(
                select(func.count(Inscricao.id)).where(Inscricao.company_id == company_id)
            )
            if (insc_count.scalar() or 0) <= 0:
                missing.append("Inscrições PGFN (qtd_inscricoes > 0)")
        if not company.valor_aberto:
            missing.append("Valor aberto")
        if not company.score_vf:
            missing.append("Score VF")
        if not company.situacao_processual:
            missing.append("Situação processual")

    elif to_stage == "Abordagem":
        if not company.situacao_cadastral_receita:
            missing.append("Situação cadastral Receita/CNPJ")
        # At least 1 Sancor query
        consultas = await db.execute(
            select(func.count(ConsultaSeguradora.id)).where(
                ConsultaSeguradora.company_id == company_id
            )
        )
        if (consultas.scalar() or 0) == 0:
            missing.append("Consulta Sancor registrada")
        # Decisor with contact
        if not company.decisor_nome:
            missing.append("Nome do decisor")
        if not company.decisor_cargo:
            missing.append("Cargo do decisor")
        has_contact = any([
            company.decisor_email,
            company.decisor_telefone,
            company.decisor_linkedin,
        ])
        if not has_contact:
            missing.append("Ao menos 1 canal de contato do decisor")

    elif to_stage == "Interesse Manifesto":
        interactions = await db.execute(
            select(func.count(Interaction.id)).where(
                Interaction.company_id == company_id
            )
        )
        if (interactions.scalar() or 0) == 0:
            missing.append("Registro de interação")
        if not company.proximo_followup:
            missing.append("Próximo follow-up definido")

    elif to_stage == "Análise Rápida":
        # Consulta with status != pendente
        consultas = await db.execute(
            select(ConsultaSeguradora).where(
                ConsultaSeguradora.company_id == company_id,
                ConsultaSeguradora.status != "pendente",
            )
        )
        if not consultas.scalars().first():
            missing.append("Consulta Sancor com retorno (status ≠ pendente)")
        # Financial data or doc request
        has_financial = any([company.faturamento, company.pl, company.capital_social])
        if not has_financial:
            missing.append("Dados financeiros mínimos (PL, faturamento ou capital social) ou solicitação formal de documentos")

    elif to_stage == "Proposta Enviada":
        # At least 1 proposal not fora_politica
        propostas = await db.execute(
            select(Proposta).where(
                Proposta.company_id == company_id,
                Proposta.flag_aderencia != "fora_politica",
            )
        )
        if not propostas.scalars().first():
            # Check if there's a NDA path (no limit case)
            docs = await db.execute(
                select(Documento).where(
                    Documento.company_id == company_id,
                    Documento.tipo == "nda",
                    Documento.status != "pendente",
                )
            )
            if not docs.scalars().first():
                missing.append("Proposta com aderência econômica, OU NDA + briefing (caso sem limite)")

    elif to_stage == "Submetido Sancor":
        # Blocking docs check
        for doc_tipo in BLOCKING_DOCS:
            doc_result = await db.execute(
                select(Documento).where(
                    Documento.company_id == company_id,
                    Documento.tipo == doc_tipo,
                    Documento.status.in_(["recebido", "enviado", "validado"]),
                )
            )
            if not doc_result.scalars().first():
                labels = {
                    "balanco_dre": "Balanço/DRE",
                    "contrato_social": "Contrato Social",
                    "ficha_judicial": "Ficha Judicial",
                }
                missing.append(f"Documento: {labels.get(doc_tipo, doc_tipo)} (status ≥ recebido)")

    elif to_stage == "Aprovado":
        consultas = await db.execute(
            select(ConsultaSeguradora).where(
                ConsultaSeguradora.company_id == company_id,
                ConsultaSeguradora.status.in_(["aprovado_automatico", "aprovado_indicativo"]),
            )
        )
        if not consultas.scalars().first():
            missing.append("Consulta Sancor com aprovação formal")

    elif to_stage == "Fechado":
        propostas = await db.execute(
            select(Proposta).where(
                Proposta.company_id == company_id,
            ).order_by(Proposta.created_at.desc())
        )
        latest = propostas.scalars().first()
        if not latest or (not latest.numero_apolice and not latest.data_emissao):
            missing.append("Número da apólice ou data de emissão na proposta")

    elif to_stage == "Receita Realizada":
        if not company.comissao_recebida:
            missing.append("Confirmação de recebimento da comissão")
        if not company.honorarios_recebidos:
            missing.append("Confirmação de recebimento dos honorários")

    return len(missing) == 0, missing
