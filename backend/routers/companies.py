from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_
from typing import Optional, List
from pydantic import BaseModel
from datetime import date, datetime
import uuid, io, pandas as pd

from backend.database import get_db
from backend.models import Company, Interaction

router = APIRouter(prefix="/api/companies", tags=["companies"])

# ── Schemas ──────────────────────────────────────────────────────────────────

class CompanyCreate(BaseModel):
    cnpj: str
    razao_social: Optional[str] = None
    uf: Optional[str] = None
    score_vf: Optional[float] = None
    prioridade: Optional[str] = None
    faixa_valor: Optional[str] = None
    valor_aberto: Optional[float] = None
    qtd_inscricoes: Optional[int] = None
    anos_pgfn: Optional[float] = None
    ano_ult_inscricao: Optional[int] = None
    situacao_processual: Optional[str] = None
    receita_principal: Optional[str] = None
    simples_nacional: Optional[bool] = False
    frente: Optional[int] = None
    seguradora_elegivel: Optional[str] = None
    responsavel: Optional[str] = None
    estagio_pipeline: Optional[str] = "Base PGFN"
    proximo_followup: Optional[date] = None
    observacoes: Optional[str] = None
    created_by: Optional[str] = None

class CompanyUpdate(BaseModel):
    razao_social: Optional[str] = None
    uf: Optional[str] = None
    score_vf: Optional[float] = None
    prioridade: Optional[str] = None
    faixa_valor: Optional[str] = None
    valor_aberto: Optional[float] = None
    qtd_inscricoes: Optional[int] = None
    anos_pgfn: Optional[float] = None
    ano_ult_inscricao: Optional[int] = None
    situacao_processual: Optional[str] = None
    receita_principal: Optional[str] = None
    simples_nacional: Optional[bool] = None
    frente: Optional[int] = None
    seguradora_elegivel: Optional[str] = None
    responsavel: Optional[str] = None
    estagio_pipeline: Optional[str] = None
    proximo_followup: Optional[date] = None
    # Seguradora
    seguradora_consultada: Optional[str] = None
    limite_aprovado: Optional[float] = None
    taxa_minima: Optional[float] = None
    status_consulta: Optional[str] = None
    obs_underwriter: Optional[str] = None
    # Financeiro
    pl: Optional[float] = None
    faturamento: Optional[float] = None
    ebitda: Optional[float] = None
    regime_tributario: Optional[str] = None
    porte: Optional[str] = None
    capital_social: Optional[float] = None
    data_balanco: Optional[date] = None
    fonte_dados: Optional[str] = None
    # Decisor
    decisor_nome: Optional[str] = None
    decisor_cargo: Optional[str] = None
    decisor_linkedin: Optional[str] = None
    decisor_email: Optional[str] = None
    decisor_email_confirmado: Optional[bool] = None
    decisor_telefone: Optional[str] = None
    # Proposta
    score_confirmado: Optional[float] = None
    valor_garantia: Optional[float] = None
    prazo_anos: Optional[int] = None
    taxa_negociada: Optional[float] = None
    premio_calculado: Optional[float] = None
    honorarios: Optional[float] = None
    receita_vf: Optional[float] = None
    status_proposta: Optional[str] = None
    data_proposta: Optional[date] = None
    numero_apolice: Optional[str] = None
    data_emissao: Optional[date] = None
    comissao_recebida: Optional[bool] = None
    honorarios_recebidos: Optional[bool] = None
    observacoes: Optional[str] = None
    enrichment_status: Optional[str] = None
    updated_by: Optional[str] = None

def company_to_dict(c: Company) -> dict:
    return {k: v for k, v in c.__dict__.items() if not k.startswith("_")}

# ── Endpoints ────────────────────────────────────────────────────────────────

@router.get("")
def list_companies(
    search: Optional[str] = None,
    frente: Optional[int] = None,
    responsavel: Optional[str] = None,
    estagio: Optional[str] = None,
    seguradora: Optional[str] = None,
    prioridade: Optional[str] = None,
    score_min: Optional[float] = None,
    score_max: Optional[float] = None,
    followup_vencido: Optional[bool] = None,
    status: Optional[str] = "active",
    limit: int = 200,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    q = db.query(Company)
    if status:
        q = q.filter(Company.status == status)
    if search:
        q = q.filter(or_(
            Company.razao_social.ilike(f"%{search}%"),
            Company.cnpj.ilike(f"%{search}%")
        ))
    if frente:
        q = q.filter(Company.frente == frente)
    if responsavel:
        q = q.filter(Company.responsavel == responsavel)
    if estagio:
        q = q.filter(Company.estagio_pipeline == estagio)
    if seguradora:
        q = q.filter(Company.seguradora_elegivel == seguradora)
    if prioridade:
        q = q.filter(Company.prioridade == prioridade)
    if score_min is not None:
        q = q.filter(Company.score_vf >= score_min)
    if score_max is not None:
        q = q.filter(Company.score_vf <= score_max)
    if followup_vencido:
        from datetime import date
        q = q.filter(Company.proximo_followup < date.today())

    total = q.count()
    companies = q.order_by(Company.score_vf.desc()).offset(offset).limit(limit).all()
    return {"total": total, "items": [company_to_dict(c) for c in companies]}

@router.post("")
def create_company(data: CompanyCreate, db: Session = Depends(get_db)):
    existing = db.query(Company).filter(Company.cnpj == data.cnpj).first()
    if existing:
        raise HTTPException(status_code=409, detail="CNPJ já cadastrado")
    c = Company(id=str(uuid.uuid4()), **data.dict())
    db.add(c)
    db.commit()
    db.refresh(c)
    return company_to_dict(c)

@router.get("/{company_id}")
def get_company(company_id: str, db: Session = Depends(get_db)):
    c = db.query(Company).filter(Company.id == company_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="Empresa não encontrada")
    result = company_to_dict(c)
    result["interactions"] = [
        {k: v for k, v in i.__dict__.items() if not k.startswith("_")}
        for i in c.interactions
    ]
    return result

@router.put("/{company_id}")
def update_company(company_id: str, data: CompanyUpdate, db: Session = Depends(get_db)):
    c = db.query(Company).filter(Company.id == company_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="Empresa não encontrada")
    for field, value in data.dict(exclude_none=True).items():
        setattr(c, field, value)
    db.commit()
    db.refresh(c)
    return company_to_dict(c)

@router.delete("/{company_id}")
def archive_company(company_id: str, db: Session = Depends(get_db)):
    c = db.query(Company).filter(Company.id == company_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="Empresa não encontrada")
    c.status = "archived"
    db.commit()
    return {"ok": True}

@router.post("/import")
async def import_companies(
    file: UploadFile = File(...),
    user: str = "sistema",
    db: Session = Depends(get_db)
):
    content = await file.read()
    imported, duplicates, errors = 0, 0, 0

    try:
        if file.filename.endswith(".xlsx"):
            xls = pd.ExcelFile(io.BytesIO(content))
            frames = []

            # Mapa de aba → seguradora e frente
            ABA_MAP = {
                'Sancor': ('Sancor', 1),
                'Berkley': ('Berkley', 2),
                'Zurich': ('Zurich/Swiss Re/Chubb', 2),
                'Base Completa': (None, None),  # usa lógica por valor
            }

            def detect_seg_frente(sheet_name):
                for key, val in ABA_MAP.items():
                    if key in sheet_name:
                        return val
                return (None, None)

            # Importa aba "Base Completa" apenas se não houver abas específicas
            sheet_names = [s for s in xls.sheet_names if 'Painel' not in s]
            has_specific = any('Sancor' in s or 'Berkley' in s or 'Zurich' in s for s in sheet_names)
            sheets_to_import = [s for s in sheet_names if 'Base Completa' not in s] if has_specific else sheet_names

            for sheet in sheets_to_import:
                seg_from_sheet, frente_from_sheet = detect_seg_frente(sheet)
                df = pd.read_excel(io.BytesIO(content), sheet_name=sheet, header=None)
                # Detecta linha de header (contém 'Empresa' ou 'CNPJ')
                header_row = None
                for i, row in df.iterrows():
                    vals = [str(v).strip() for v in row.values]
                    if any("Empresa" in v or "CNPJ" in v for v in vals):
                        header_row = i
                        break
                if header_row is None:
                    continue
                df.columns = df.iloc[header_row]
                df = df.iloc[header_row+1:].reset_index(drop=True)
                df['_seg_sheet'] = seg_from_sheet
                df['_frente_sheet'] = frente_from_sheet
                frames.append(df)

            if frames:
                df = pd.concat(frames, ignore_index=True)
            else:
                return {"imported": 0, "duplicates": 0, "errors": 0, "message": "Nenhuma aba válida encontrada"}
        elif file.filename.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(content), encoding="utf-8-sig")
        else:
            raise HTTPException(status_code=400, detail="Formato não suportado. Use .xlsx ou .csv")

        # Normaliza nomes de colunas
        col_map = {
            "Empresa": "razao_social", "CNPJ Completo": "cnpj", "CNPJ": "cnpj",
            "UF": "uf", "Score": "score_vf", "Prioridade": "prioridade",
            "Faixa Valor": "faixa_valor", "Valor Aberto (R$)": "valor_aberto",
            "Qtd. Inscricoes": "qtd_inscricoes", "Qtd. Inscrições": "qtd_inscricoes",
            "Anos na PGFN": "anos_pgfn", "Ano Ult. Inscricao": "ano_ult_inscricao",
            "Ano Ult. Inscrição": "ano_ult_inscricao", "Receita Principal": "receita_principal",
            "Situacoes Presentes": "situacao_processual", "Situações Presentes": "situacao_processual",
            "Simples Nacional": "simples_nacional", "Observacoes": "observacoes",
            "Observações": "observacoes",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

        for _, row in df.iterrows():
            try:
                cnpj = str(row.get("cnpj", "")).strip()
                if not cnpj or cnpj == "nan":
                    continue
                existing = db.query(Company).filter(Company.cnpj == cnpj).first()
                if existing:
                    duplicates += 1
                    continue

                def safe(field, cast=None):
                    val = row.get(field)
                    if val is None or str(val) == "nan":
                        return None
                    return cast(val) if cast else str(val)

                score = safe("score_vf", float)
                valor = safe("valor_aberto", float) or 0

                # Seguradora e frente: vem da aba; fallback por valor
                seg = row.get('_seg_sheet')
                frente = row.get('_frente_sheet')
                if not seg:
                    if valor <= 20_000_000:
                        seg = "Sancor"
                        frente = 1
                    elif valor <= 30_000_000:
                        seg = "Berkley"
                        frente = 2
                    else:
                        seg = "Zurich/Swiss Re/Chubb"
                        frente = 2

                c = Company(
                    id=str(uuid.uuid4()),
                    cnpj=cnpj,
                    razao_social=safe("razao_social"),
                    uf=safe("uf"),
                    score_vf=score,
                    prioridade=safe("prioridade"),
                    faixa_valor=safe("faixa_valor"),
                    valor_aberto=valor or None,
                    qtd_inscricoes=safe("qtd_inscricoes", int),
                    anos_pgfn=safe("anos_pgfn", float),
                    ano_ult_inscricao=safe("ano_ult_inscricao", int),
                    situacao_processual=safe("situacao_processual"),
                    receita_principal=safe("receita_principal"),
                    simples_nacional=str(row.get("simples_nacional", "")).lower() in ["sim", "s", "true", "1"],
                    frente=frente,
                    seguradora_elegivel=seg,
                    estagio_pipeline="Base PGFN",
                    enrichment_status="pgfn_only",
                    observacoes=safe("observacoes"),
                    created_by=user,
                )
                db.add(c)
                imported += 1
            except Exception:
                errors += 1

        db.commit()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "imported": imported,
        "duplicates": duplicates,
        "errors": errors,
        "message": f"{imported} empresas importadas, {duplicates} duplicatas ignoradas, {errors} erros"
    }
