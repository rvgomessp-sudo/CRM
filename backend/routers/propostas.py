"""CRUD endpoints for proposals + VF Solver."""

import uuid
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..models import (
    Proposta, Company,
    PropostaCreate, PropostaUpdate, PropostaResponse, SolverResult,
)

router = APIRouter(prefix="/api/propostas", tags=["propostas"])


def _calculate_proposta(
    valor_garantia: Decimal,
    prazo_anos: int,
    taxa: Decimal,
    comissao_pct: Decimal,
    honorarios: Decimal,
    teto_percepcao: Optional[Decimal] = None,
) -> dict:
    """Calculate all derived proposal fields."""
    premio_bruto = valor_garantia * taxa * prazo_anos
    premio_liquido = premio_bruto  # simplified: premio_liquido = premio_bruto for Sancor
    comissao_valor = premio_bruto * comissao_pct / Decimal("100")
    receita_vf_total = comissao_valor + honorarios

    # Flag de aderência: comissão + honorários > prêmio líquido
    if comissao_valor + honorarios > premio_liquido:
        margin = (comissao_valor + honorarios - premio_liquido) / premio_liquido if premio_liquido else Decimal("1")
        flag_aderencia = "aderente" if margin > Decimal("0.1") else "atencao"
    else:
        flag_aderencia = "fora_politica"

    # Relação honorários/prêmio
    relacao_hon_premio = (honorarios / premio_bruto) if premio_bruto else Decimal("0")

    custo_cliente = premio_bruto + honorarios

    return {
        "premio_bruto": premio_bruto.quantize(Decimal("0.01")),
        "premio_liquido": premio_liquido.quantize(Decimal("0.01")),
        "comissao_valor": comissao_valor.quantize(Decimal("0.01")),
        "receita_vf_total": receita_vf_total.quantize(Decimal("0.01")),
        "flag_aderencia": flag_aderencia,
        "relacao_hon_premio": relacao_hon_premio.quantize(Decimal("0.01")),
        "custo_cliente": custo_cliente.quantize(Decimal("0.01")),
    }


@router.get("/{company_id}", response_model=List[PropostaResponse])
async def list_propostas(
    company_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Proposta)
        .where(Proposta.company_id == company_id)
        .order_by(Proposta.created_at.desc())
    )
    return result.scalars().all()


@router.post("", response_model=PropostaResponse, status_code=201)
async def create_proposta(
    data: PropostaCreate,
    db: AsyncSession = Depends(get_db),
):
    # Verify company
    company_q = await db.execute(select(Company).where(Company.id == data.company_id))
    if not company_q.scalar_one_or_none():
        raise HTTPException(404, "Company not found")

    obj = Proposta(**data.model_dump())

    # Auto-calculate if we have the inputs
    if data.valor_garantia and data.prazo_anos and data.taxa and data.comissao_pct is not None:
        honorarios = data.honorarios or Decimal("0")
        calc = _calculate_proposta(
            data.valor_garantia, data.prazo_anos, data.taxa,
            data.comissao_pct, honorarios, data.teto_percepcao,
        )
        obj.premio_bruto = calc["premio_bruto"]
        obj.premio_liquido = calc["premio_liquido"]
        obj.comissao_valor = calc["comissao_valor"]
        obj.receita_vf_total = calc["receita_vf_total"]
        obj.flag_aderencia = calc["flag_aderencia"]
        obj.relacao_hon_premio = calc["relacao_hon_premio"]

    db.add(obj)
    await db.flush()
    await db.refresh(obj)
    return obj


@router.put("/{proposta_id}", response_model=PropostaResponse)
async def update_proposta(
    proposta_id: uuid.UUID,
    update: PropostaUpdate,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Proposta).where(Proposta.id == proposta_id))
    proposta = result.scalar_one_or_none()
    if not proposta:
        raise HTTPException(404, "Proposta not found")

    data = update.model_dump(exclude_unset=True)
    for key, value in data.items():
        setattr(proposta, key, value)

    # Recalculate if core fields changed
    if any(k in data for k in ("valor_garantia", "prazo_anos", "taxa", "comissao_pct", "honorarios")):
        vg = proposta.valor_garantia
        pa = proposta.prazo_anos
        tx = proposta.taxa
        cp = proposta.comissao_pct
        hon = proposta.honorarios or Decimal("0")
        if vg and pa and tx and cp is not None:
            calc = _calculate_proposta(vg, pa, tx, cp, hon, proposta.teto_percepcao)
            proposta.premio_bruto = calc["premio_bruto"]
            proposta.premio_liquido = calc["premio_liquido"]
            proposta.comissao_valor = calc["comissao_valor"]
            proposta.receita_vf_total = calc["receita_vf_total"]
            proposta.flag_aderencia = calc["flag_aderencia"]
            proposta.relacao_hon_premio = calc["relacao_hon_premio"]

    await db.flush()
    await db.refresh(proposta)
    return proposta


@router.get("/solver/calculate", response_model=List[SolverResult])
async def solver_calculate(
    valor_garantia: Decimal = Query(...),
    prazo_anos: int = Query(...),
    comissao_pct: Decimal = Query(default=Decimal("20")),
    teto_percepcao: Decimal = Query(default=Decimal("100000")),
):
    """VF Solver: sensitivity table from 0.50% to 1.50% in 0.25% steps."""
    results = []
    taxa = Decimal("0.0050")
    step = Decimal("0.0025")

    while taxa <= Decimal("0.0150"):
        premio_bruto = valor_garantia * taxa * prazo_anos
        comissao_valor = premio_bruto * comissao_pct / Decimal("100")

        # Honorários = teto - prêmio (when positive)
        honorarios = max(teto_percepcao - premio_bruto, Decimal("0"))

        receita_vf_total = comissao_valor + honorarios
        custo_cliente = premio_bruto + honorarios
        premio_liquido = premio_bruto

        relacao_hon_premio = (honorarios / premio_bruto) if premio_bruto else Decimal("0")

        if comissao_valor + honorarios > premio_liquido:
            margin = (comissao_valor + honorarios - premio_liquido) / premio_liquido if premio_liquido else Decimal("1")
            flag = "aderente" if margin > Decimal("0.1") else "atencao"
        else:
            flag = "fora_politica"

        results.append(SolverResult(
            taxa=taxa,
            premio_bruto=premio_bruto.quantize(Decimal("0.01")),
            premio_liquido=premio_liquido.quantize(Decimal("0.01")),
            comissao_valor=comissao_valor.quantize(Decimal("0.01")),
            honorarios=honorarios.quantize(Decimal("0.01")),
            receita_vf_total=receita_vf_total.quantize(Decimal("0.01")),
            custo_cliente=custo_cliente.quantize(Decimal("0.01")),
            relacao_hon_premio=relacao_hon_premio.quantize(Decimal("0.01")),
            flag_aderencia=flag,
        ))
        taxa += step

    return results
