from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
import httpx, os

from backend.database import get_db
from backend.models import Setting

router = APIRouter(prefix="/api/enrich", tags=["enrich"])

def get_setting(db: Session, key: str) -> str | None:
    s = db.query(Setting).filter(Setting.key == key).first()
    return s.value if s else None

@router.get("/cnpj/{cnpj}")
async def enrich_cnpj(cnpj: str, db: Session = Depends(get_db)):
    """BrasilAPI / CNPJ.ws — regime, capital, sócios, porte"""
    cnpj_clean = cnpj.replace(".", "").replace("/", "").replace("-", "")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_clean}")
            if r.status_code == 200:
                data = r.json()
                return {
                    "status": "ok",
                    "source": "BrasilAPI",
                    "razao_social": data.get("razao_social"),
                    "uf": data.get("uf"),
                    "porte": data.get("porte"),
                    "capital_social": data.get("capital_social"),
                    "regime_tributario": data.get("opcao_pelo_simples") and "Simples Nacional" or None,
                    "simples_nacional": data.get("opcao_pelo_simples", False),
                    "situacao_cadastral": data.get("descricao_situacao_cadastral"),
                    "socios": data.get("qsa", []),
                }
    except Exception as e:
        pass
    return {"status": "error", "message": "CNPJ não encontrado na BrasilAPI"}

@router.get("/financeiro/{cnpj}")
async def enrich_financeiro(cnpj: str, db: Session = Depends(get_db)):
    """Serasa / Neoway — PL, faturamento (requer API key configurada)"""
    api_key = get_setting(db, "SERASA_API_KEY")
    if not api_key:
        return {
            "status": "not_configured",
            "mock": True,
            "message": "Configure SERASA_API_KEY nas configurações para ativar enriquecimento financeiro"
        }
    # Placeholder — implementar quando contratar Serasa/Neoway
    return {"status": "not_implemented", "message": "Integração Serasa/Neoway pendente de implementação"}

@router.get("/decisor/{cnpj}")
async def enrich_decisor(cnpj: str, db: Session = Depends(get_db)):
    """Apollo.io — e-mails verificados (requer API key)"""
    api_key = get_setting(db, "APOLLO_API_KEY")
    if not api_key:
        return {
            "status": "not_configured",
            "mock": True,
            "message": "Configure APOLLO_API_KEY nas configurações para buscar decisores"
        }
    return {"status": "not_implemented", "message": "Integração Apollo.io pendente de implementação"}

@router.get("/seguradora/{cnpj}/{seguradora}")
async def enrich_seguradora(cnpj: str, seguradora: str, db: Session = Depends(get_db)):
    """Consulta limite e taxa na seguradora (requer endpoint configurado)"""
    key = f"{seguradora.upper()}_API_URL"
    url = get_setting(db, key)
    if not url:
        return {
            "status": "not_configured",
            "mock": True,
            "message": f"Configure {key} nas configurações para consultar {seguradora}"
        }
    return {"status": "not_implemented", "message": f"Integração {seguradora} pendente de implementação"}
