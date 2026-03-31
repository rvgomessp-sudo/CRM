#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Exportar para VF CRM — Ponte entre Pipeline Python e CRM HTML
==============================================================
Le o CSV agregado por CNPJ raiz (saida do segmentacao_universo.py)
e gera um XLSX com abas Sancor / Berkley / Zurich, pronto para
importar no VF_CRM.html via botao "Importar Planilha PGFN (.xlsx)".

Tambem gera um JSON compativel com o backup do CRM (importavel direto).

Uso:
    python exportar_para_crm.py
    python exportar_para_crm.py --entrada caminho/para/pgfn_agregado_cnpj_raiz_perfil.csv
    python exportar_para_crm.py --entrada caminho/para/csv --saida VF_PGFN_Carteira.xlsx
"""

import pandas as pd
import numpy as np
import json
import argparse
import os
from pathlib import Path
from datetime import datetime

# ============================================================================
# CONFIGURACAO
# ============================================================================

DATA_DIR = Path(os.environ.get("PGFN_DATA_DIR", r"C:\Rodrigo\BasePGFN\2026"))
PIPELINE_OUTPUT = DATA_DIR / "outputs_pipeline_a"

ARQUIVO_AGREGADO = "pgfn_agregado_cnpj_raiz_perfil.csv"

# Regras de segmentacao por seguradora (espelham VF_CRM.html)
REGRAS = {
    'Sancor': {
        'valor_max': 20_000_000,
        'pl_min': 0,           # PL >= 20M ideal, mas sem filtro duro no CSV
        'faturamento_min': 0,
        'descricao': 'Frente 1 — Volume, alta rotatividade',
    },
    'Berkley': {
        'valor_min': 5_000_000,
        'valor_max': 30_000_000,
        'faturamento_min': 0,  # >= 100M ideal
        'descricao': 'Intermediaria — Medio porte',
    },
    'Zurich/Swiss/Chubb': {
        'valor_min': 30_000_000,
        'valor_max': float('inf'),
        'faturamento_min': 0,  # >= 300M ideal
        'descricao': 'Frente 2 — Grande porte, artesanal',
    },
}


def classificar_faixa(valor):
    """Classifica valor em faixa comercial."""
    if valor < 1_000_000:
        return 'R$<1M'
    elif valor < 5_000_000:
        return 'R$1-5M'
    elif valor < 10_000_000:
        return 'R$5-10M'
    elif valor < 20_000_000:
        return 'R$10-20M'
    elif valor < 30_000_000:
        return 'R$20-30M'
    elif valor < 50_000_000:
        return 'R$30-50M'
    else:
        return 'R$50M+'


def calcular_score_pgfn(row):
    """
    Score simplificado 0-100 baseado nos dados PGFN disponiveis.
    Quanto maior, mais atrativo para seguro garantia.
    """
    score = 50  # base

    # Valor total (empresas maiores = mais atrativas)
    valor = row.get('VALOR_TOTAL_CNPJ', 0) or 0
    if valor >= 50_000_000:
        score += 15
    elif valor >= 10_000_000:
        score += 10
    elif valor >= 5_000_000:
        score += 5
    elif valor < 1_000_000:
        score -= 15

    # Taxa de ajuizamento alta = oportunidade
    taxa_aj = row.get('TAXA_AJUIZAMENTO_CNPJ', 0) or 0
    if taxa_aj >= 0.8:
        score += 10
    elif taxa_aj >= 0.5:
        score += 5

    # Sem garantia = oportunidade maxima
    taxa_gar = row.get('TAXA_GARANTIA_CNPJ', 0) or 0
    if taxa_gar == 0:
        score += 15
    elif taxa_gar < 0.3:
        score += 10
    elif taxa_gar > 0.8:
        score -= 10

    # Porte proxy
    porte = row.get('PORTE_PROXY', '')
    if porte == 'ALTO':
        score += 10
    elif porte == 'MEDIO':
        score += 5

    # Crescimento recente = atividade
    if row.get('FLAG_CRESCIMENTO_RECENTE', 0) == 1:
        score += 5

    return max(0, min(100, score))


def determinar_seguradora(valor_total):
    """Determina seguradora alvo pelo valor da divida."""
    if valor_total >= 30_000_000:
        return 'Zurich/Swiss/Chubb'
    elif valor_total >= 5_000_000:
        return 'Berkley'
    else:
        return 'Sancor'


def determinar_situacoes(row):
    """Infere situacoes presentes a partir dos dados agregados."""
    partes = []
    total_aj = row.get('TOTAL_AJUIZADO_CNPJ', 0) or 0
    total_gar = row.get('TOTAL_GARANTIDAS_CNPJ', 0) or 0
    total_insc = row.get('TOTAL_INSCRICOES_CNPJ', 0) or 0

    if total_aj > 0 and total_gar == 0:
        partes.append('AJUIZADA_SEM_GAR')
    elif total_aj > 0 and total_gar > 0 and total_gar < total_aj:
        partes.append('AJUIZADA_SEM_GAR')
        partes.append('AJUIZADA_COM_GAR')
    elif total_aj > 0 and total_gar >= total_aj:
        partes.append('AJUIZADA_COM_GAR')

    if total_insc > total_aj:
        partes.append('EM_ABERTO')

    return '; '.join(partes) if partes else 'EM_ABERTO'


def calcular_anos_pgfn(row):
    """Calcula anos na PGFN baseado na inscricao mais antiga."""
    data_min = row.get('INSCRICAO_MAIS_ANTIGA')
    if pd.isna(data_min) or data_min is None:
        return ''
    try:
        if isinstance(data_min, str):
            data_min = pd.to_datetime(data_min, errors='coerce')
        if pd.isna(data_min):
            return ''
        delta = (datetime.now() - data_min).days / 365.25
        return str(round(delta, 1))
    except Exception:
        return ''


def carregar_agregado(caminho):
    """Carrega o CSV agregado por CNPJ raiz."""
    print(f"Lendo: {caminho}")

    # Tentar separadores comuns
    for sep in [';', ',', '\t']:
        try:
            df = pd.read_csv(caminho, sep=sep, encoding='utf-8-sig', low_memory=False)
            if len(df.columns) > 3:
                print(f"  OK: {len(df):,} CNPJs raiz, {len(df.columns)} colunas (sep='{sep}')")
                return df
        except Exception:
            continue

    raise ValueError(f"Nao foi possivel ler {caminho}. Verifique formato e encoding.")


def transformar_para_crm(df):
    """
    Transforma DataFrame agregado no formato esperado pelo VF_CRM.html.
    Retorna DataFrame com colunas do CRM.
    """
    registros = []

    for _, row in df.iterrows():
        valor_total = row.get('VALOR_TOTAL_CNPJ', 0)
        if pd.isna(valor_total):
            valor_total = 0

        # Filtro: so empresas com valor >= 1M (ticket minimo CRM)
        if valor_total < 1_000_000:
            continue

        seguradora = determinar_seguradora(valor_total)
        score = calcular_score_pgfn(row)
        situacoes = determinar_situacoes(row)
        urgente = 'AJUIZADA_SEM_GAR' in situacoes

        registro = {
            'Empresa': row.get('NOME_DEVEDOR', ''),
            'CNPJ Raiz': str(row.get('CNPJ_RAIZ', '')),
            'CNPJ Completo': str(row.get('CNPJ_RAIZ', '')),
            'UF': row.get('UF_PRINCIPAL', ''),
            'Valor Aberto (R$)': valor_total,
            'Faixa Valor': classificar_faixa(valor_total),
            'Score': score,
            'Anos na PGFN': calcular_anos_pgfn(row),
            'Situacoes Presentes': situacoes,
            'Receita Principal': row.get('GRUPO_TRIB_PREDOMINANTE_CNPJ', ''),
            'Total Inscricoes': row.get('TOTAL_INSCRICOES_CNPJ', 0),
            'Total Ajuizado': row.get('TOTAL_AJUIZADO_CNPJ', 0),
            'Total Garantidas': row.get('TOTAL_GARANTIDAS_CNPJ', 0),
            'Valor Nao Garantido': row.get('VALOR_TOTAL_NAO_GARANTIDO_CNPJ', 0),
            'Taxa Ajuizamento': row.get('TAXA_AJUIZAMENTO_CNPJ', 0),
            'Taxa Garantia': row.get('TAXA_GARANTIA_CNPJ', 0),
            'Porte Proxy': row.get('PORTE_PROXY', ''),
            'Seguradora Alvo': seguradora,
            'Responsavel V&F': 'Anna',
            'Estagio Pipeline': 'Identificado',
            'Proximo Follow-up': '',
            'Observacoes': '',
            'Simples Nacional': 'Nao',  # ja filtrado no pipeline
        }

        registros.append(registro)

    df_crm = pd.DataFrame(registros)
    df_crm = df_crm.sort_values('Valor Aberto (R$)', ascending=False)

    return df_crm


def exportar_xlsx(df_crm, caminho_saida):
    """
    Exporta XLSX com abas por seguradora, compativel com VF_CRM.html.
    """
    with pd.ExcelWriter(caminho_saida, engine='openpyxl') as writer:
        for seguradora in ['Sancor', 'Berkley', 'Zurich/Swiss/Chubb']:
            df_seg = df_crm[df_crm['Seguradora Alvo'] == seguradora].copy()
            # Nome da aba (max 31 chars no Excel)
            nome_aba = seguradora.replace('/', '-')
            df_seg.to_excel(writer, sheet_name=nome_aba, index=False)
            print(f"  Aba '{nome_aba}': {len(df_seg):,} empresas")

        # Aba completa
        df_crm.to_excel(writer, sheet_name='Base Completa', index=False)
        print(f"  Aba 'Base Completa': {len(df_crm):,} empresas")

    print(f"\n[OK] XLSX salvo: {caminho_saida}")


def exportar_json_crm(df_crm, caminho_saida):
    """
    Exporta JSON no formato de backup do VF_CRM.html (importavel direto).
    """
    hoje = datetime.now().strftime('%Y-%m-%d')
    empresas = []

    for _, row in df_crm.iterrows():
        sit = row.get('Situacoes Presentes', '')
        empresas.append({
            'id': f"pgfn_{row.get('CNPJ Completo', '')}_{len(empresas)}",
            'nome': str(row.get('Empresa', '')),
            'cnpj': str(row.get('CNPJ Completo', '')),
            'uf': str(row.get('UF', '')),
            'setor': '',
            'email': '',
            'tel': '',
            'valor_divida': str(row.get('Valor Aberto (R$)', 0)),
            'faixa': str(row.get('Faixa Valor', '')),
            'score_pgfn': str(row.get('Score', '')),
            'anos_pgfn': str(row.get('Anos na PGFN', '')),
            'situacao': str(sit).split(';')[0].strip() if sit else 'EM_ABERTO',
            'receita_principal': str(row.get('Receita Principal', '')),
            'faturamento': '',
            'pl': '',
            'ebitda': '',
            'regime': 'Lucro Real',
            'fonte': 'Pipeline PGFN',
            'data_enriquecimento': hoje,
            'estagio': 'identificado',
            'seguradora': str(row.get('Seguradora Alvo', 'A definir')),
            'resp_inst': 'Anna',
            'resp_tec': 'Rodrigo',
            'data_entrada': hoje,
            'followup': '',
            'urgente': 'AJUIZADA_SEM_GAR' in str(sit),
            'notas': f"Importado do Pipeline Python. "
                     f"Inscricoes: {row.get('Total Inscricoes', 0)}, "
                     f"Ajuizadas: {row.get('Total Ajuizado', 0)}, "
                     f"Garantidas: {row.get('Total Garantidas', 0)}. "
                     f"Porte: {row.get('Porte Proxy', '?')}.",
            'updated': hoje,
        })

    backup = {
        'user': 'Rodrigo',
        'empresas': empresas,
        'docs': [],
        'cfg': {
            'emp': 'Vazquez & Fonseca',
            'cnpj': '',
            'oab': '',
            'email': '',
            'tel': '',
            'sheet_id': '',
            'script_url': '',
            'sigilo': 'Este documento e CONFIDENCIAL e de uso exclusivo do destinatario identificado.',
            'rodape': 'V&F - Vazquez & Fonseca - Seguro Garantia Tributario',
        }
    }

    with open(caminho_saida, 'w', encoding='utf-8') as f:
        json.dump(backup, f, ensure_ascii=False, indent=2)

    print(f"[OK] JSON CRM salvo: {caminho_saida} ({len(empresas)} empresas)")


def exportar_csv_crm(df_crm, caminho_saida):
    """
    Exporta CSV simples compativel com import do CRM.
    """
    df_crm.to_csv(caminho_saida, sep=',', index=False, encoding='utf-8-sig')
    print(f"[OK] CSV salvo: {caminho_saida}")


def main():
    parser = argparse.ArgumentParser(description='Exportar dados PGFN para VF CRM')
    parser.add_argument('--entrada', type=str, default=None,
                        help='Caminho do CSV agregado (pgfn_agregado_cnpj_raiz_perfil.csv)')
    parser.add_argument('--saida', type=str, default=None,
                        help='Nome do arquivo XLSX de saida')
    parser.add_argument('--formato', choices=['xlsx', 'json', 'csv', 'todos'], default='todos',
                        help='Formato(s) de saida (default: todos)')
    parser.add_argument('--limite', type=int, default=0,
                        help='Limitar numero de empresas exportadas (0 = sem limite)')
    args = parser.parse_args()

    # Determinar arquivo de entrada
    if args.entrada:
        caminho_entrada = Path(args.entrada)
    else:
        caminho_entrada = PIPELINE_OUTPUT / ARQUIVO_AGREGADO

    if not caminho_entrada.exists():
        print(f"ERRO: Arquivo nao encontrado: {caminho_entrada}")
        print(f"\nExecute primeiro o pipeline de segmentacao:")
        print(f"  python segmentacao_universo.py")
        print(f"\nOu especifique o caminho manualmente:")
        print(f"  python exportar_para_crm.py --entrada /caminho/para/csv")
        return

    # Carregar e transformar
    print("=" * 70)
    print("EXPORTAR PARA VF CRM")
    print("=" * 70)

    df = carregar_agregado(caminho_entrada)
    df_crm = transformar_para_crm(df)

    if args.limite > 0:
        df_crm = df_crm.head(args.limite)
        print(f"\n  Limitado a {args.limite} empresas (top por valor)")

    print(f"\n  Total para CRM: {len(df_crm):,} empresas (ticket >= R$ 1M)")

    # Resumo por seguradora
    print(f"\n  Distribuicao por seguradora:")
    for seg in ['Sancor', 'Berkley', 'Zurich/Swiss/Chubb']:
        n = len(df_crm[df_crm['Seguradora Alvo'] == seg])
        print(f"    {seg}: {n:,}")

    # Diretorio de saida
    output_dir = caminho_entrada.parent
    base_name = args.saida or f"VF_PGFN_Carteira_{datetime.now().strftime('%Y%m%d')}"
    base_name = base_name.replace('.xlsx', '').replace('.json', '').replace('.csv', '')

    print()

    # Exportar
    if args.formato in ('xlsx', 'todos'):
        exportar_xlsx(df_crm, output_dir / f"{base_name}.xlsx")

    if args.formato in ('json', 'todos'):
        exportar_json_crm(df_crm, output_dir / f"{base_name}_CRM_Backup.json")

    if args.formato in ('csv', 'todos'):
        exportar_csv_crm(df_crm, output_dir / f"{base_name}.csv")

    print(f"\n{'=' * 70}")
    print(f"PRONTO! Importar no CRM:")
    print(f"  1. Abrir VF_CRM.html no navegador")
    print(f"  2. Ir em 'Config & Sheets'")
    print(f"  3. Clicar 'Selecionar arquivo .xlsx' e escolher {base_name}.xlsx")
    print(f"     OU clicar 'Importar JSON' e escolher {base_name}_CRM_Backup.json")
    print(f"{'=' * 70}")


if __name__ == '__main__':
    main()
