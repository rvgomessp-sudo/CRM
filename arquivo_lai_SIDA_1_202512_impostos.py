"""
Analise estrategica de impostos na base PGFN SIDA_1.

Gera estatisticas por tributo, distribuicao temporal, top empresas
e cruzamento ajuizado x situacao.
"""

import pandas as pd
import sys

import config
from utils import fmt_brl, ler_base_pgfn, converter_valor_brl, filtrar_tributos


def main():
    arquivo = config.BASES[0][0]
    print("  Lendo base PGFN...")

    try:
        df = ler_base_pgfn(arquivo, colunas=config.COLUNAS_COMPLETAS)
    except FileNotFoundError as e:
        print(f"  ERRO: {e}")
        sys.exit(1)

    # Limpeza
    df["VALOR_CONSOLIDADO"] = converter_valor_brl(df["VALOR_CONSOLIDADO"])
    df["DATA_INSCRICAO"] = pd.to_datetime(df["DATA_INSCRICAO"], errors="coerce", dayfirst=True)
    df["ANO_INSCRICAO"] = df["DATA_INSCRICAO"].dt.year

    # Filtro de tributos
    df = filtrar_tributos(df)
    print(f"  Registros apos filtro estrategico: {len(df):,}")

    # 1. Agregado por tributo
    print("\n  TRIBUTOS - INSCRICOES, DIVIDA TOTAL, MEDIA, CNPJs")
    tributos = (
        df.groupby("RECEITA_PRINCIPAL")
        .agg(
            QTD_INSCRICOES=("NUMERO_INSCRICAO", "count"),
            DIVIDA_TOTAL=("VALOR_CONSOLIDADO", "sum"),
            DIVIDA_MEDIA=("VALOR_CONSOLIDADO", "mean"),
            CNPJS_DISTINTOS=("CPF_CNPJ", "nunique")
        )
        .sort_values("DIVIDA_TOTAL", ascending=False)
    )
    print(tributos)

    # 2. Distribuicao por ano
    print("\n  DISTRIBUICAO DA DIVIDA POR ANO (R$)")
    ano = df.groupby("ANO_INSCRICAO")["VALOR_CONSOLIDADO"].sum().sort_index()
    print(ano)

    # 3. Top 10 empresas
    print("\n  TOP 10 EMPRESAS - MAIOR DIVIDA CONSOLIDADA")
    top_empresas = (
        df.groupby(["CPF_CNPJ", "NOME_DEVEDOR"])
        .agg(DIVIDA_TOTAL=("VALOR_CONSOLIDADO", "sum"), QTD_INSCRICOES=("NUMERO_INSCRICAO", "count"))
        .sort_values("DIVIDA_TOTAL", ascending=False)
        .head(10)
    )
    print(top_empresas)

    # 4. Ajuizado x nao ajuizado
    print("\n  AJUIZADO x NAO AJUIZADO - POR VALOR E SITUACAO")
    ajuizado = (
        df.groupby(["INDICADOR_AJUIZADO", "SITUACAO_INSCRICAO"])
        .agg(DIVIDA_TOTAL=("VALOR_CONSOLIDADO", "sum"), QTD_INSCRICOES=("NUMERO_INSCRICAO", "count"))
        .sort_values("DIVIDA_TOTAL", ascending=False)
    )
    print(ajuizado)

    print("\n  ANALISE ESTRATEGICA CONCLUIDA")


if __name__ == "__main__":
    main()
