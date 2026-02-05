"""
Analise PGFN: PJ + Tributos estruturais.

Analise exploratoria da base SIDA_1 com foco em volume por tributo,
situacao, ajuizamento, tipo de garantia e mercado alvo.
"""

import pandas as pd
import sys

import config
from utils import fmt_brl, pipeline_base


def main():
    print("=" * 70)
    print("  LENDO BASE PGFN COMPLETA...")
    print("=" * 70)

    arquivo = config.BASES[0][0]
    try:
        df = pipeline_base(arquivo, colunas=config.COLUNAS_COMPLETAS)
    except FileNotFoundError as e:
        print(f"  ERRO: {e}")
        sys.exit(1)

    total_divida = df["VALOR_CONSOLIDADO"].sum()
    media_divida = df["VALOR_CONSOLIDADO"].mean()
    mediana_divida = df["VALOR_CONSOLIDADO"].median()

    # Resumo geral
    print("\n" + "=" * 70)
    print("  RESUMO GERAL - PJ + TRIBUTOS MEDIO/GRANDE PORTE")
    print("=" * 70)
    print(f"\n  VOLUME:")
    print(f"    Registros (inscricoes): {len(df):,}")
    print(f"    CNPJs unicos: {df['CPF_CNPJ'].nunique():,}")
    print(f"\n  DIVIDA TOTAL: {fmt_brl(total_divida)}")
    print(f"  DIVIDA MEDIA por inscricao: {fmt_brl(media_divida)}")
    print(f"  DIVIDA MEDIANA por inscricao: {fmt_brl(mediana_divida)}")

    # Por tributo
    print("\n" + "=" * 70)
    print("  VOLUME POR TRIBUTO")
    print("=" * 70)

    por_tributo = (
        df.groupby("RECEITA_PRINCIPAL")
        .agg(
            QTD=("NUMERO_INSCRICAO", "count"),
            DIVIDA_TOTAL=("VALOR_CONSOLIDADO", "sum"),
            DIVIDA_MEDIA=("VALOR_CONSOLIDADO", "mean"),
            CNPJS=("CPF_CNPJ", "nunique")
        )
        .sort_values("DIVIDA_TOTAL", ascending=False)
    )

    print(f"\n{'TRIBUTO':<60} {'QTD':>10} {'DIVIDA TOTAL':>22} {'CNPJs':>10}")
    print("-" * 105)
    for idx, row in por_tributo.head(20).iterrows():
        tributo_nome = str(idx)[:60]
        print(f"{tributo_nome:<60} {int(row['QTD']):>10,} {fmt_brl(row['DIVIDA_TOTAL']):>22} {int(row['CNPJS']):>10,}")
    print("-" * 105)
    print(f"{'TOTAL':<60} {len(df):>10,} {fmt_brl(total_divida):>22} {df['CPF_CNPJ'].nunique():>10,}")

    # Por situacao
    print("\n" + "=" * 70)
    print("  TOP 20 SITUACOES (por divida)")
    print("=" * 70)

    por_situacao = (
        df.groupby("SITUACAO_INSCRICAO")
        .agg(QTD=("NUMERO_INSCRICAO", "count"), DIVIDA_TOTAL=("VALOR_CONSOLIDADO", "sum"))
        .sort_values("DIVIDA_TOTAL", ascending=False)
        .head(20)
    )

    print(f"\n{'SITUACAO':<65} {'QTD':>10} {'DIVIDA TOTAL':>22}")
    print("-" * 100)
    for idx, row in por_situacao.iterrows():
        print(f"{str(idx)[:65]:<65} {int(row['QTD']):>10,} {fmt_brl(row['DIVIDA_TOTAL']):>22}")

    # Ajuizado x Nao ajuizado
    print("\n" + "=" * 70)
    print("  AJUIZADO x NAO AJUIZADO")
    print("=" * 70)

    ajuizado_stats = df.groupby("AJUIZADO_BIN").agg(
        QTD=("NUMERO_INSCRICAO", "count"),
        DIVIDA_TOTAL=("VALOR_CONSOLIDADO", "sum"),
        CNPJS=("CPF_CNPJ", "nunique")
    )

    print(f"\n{'STATUS':<20} {'QTD':>12} {'DIVIDA TOTAL':>25} {'CNPJs':>12}")
    print("-" * 72)
    for idx, row in ajuizado_stats.iterrows():
        status = "AJUIZADO" if idx else "NAO AJUIZADO"
        print(f"{status:<20} {int(row['QTD']):>12,} {fmt_brl(row['DIVIDA_TOTAL']):>25} {int(row['CNPJS']):>12,}")

    # Garantia
    print("\n" + "=" * 70)
    print("  TIPO DE GARANTIA")
    print("=" * 70)

    garantia_stats = df.groupby("GARANTIA_ATUAL").agg(
        QTD=("NUMERO_INSCRICAO", "count"),
        DIVIDA_TOTAL=("VALOR_CONSOLIDADO", "sum"),
        CNPJS=("CPF_CNPJ", "nunique")
    ).sort_values("DIVIDA_TOTAL", ascending=False)

    print(f"\n{'GARANTIA':<20} {'QTD':>12} {'DIVIDA TOTAL':>25} {'CNPJs':>12} {'%':>8}")
    print("-" * 80)
    for idx, row in garantia_stats.iterrows():
        pct = (row['QTD'] / len(df)) * 100
        print(f"{idx:<20} {int(row['QTD']):>12,} {fmt_brl(row['DIVIDA_TOTAL']):>25} "
              f"{int(row['CNPJS']):>12,} {pct:>7.2f}%")

    # Cruzamento
    print("\n" + "=" * 70)
    print("  CRUZAMENTO: AJUIZADO x GARANTIA")
    print("=" * 70)

    cross = pd.crosstab(
        df["AJUIZADO_BIN"].map({True: "AJUIZADO", False: "NAO AJUIZADO"}),
        df["GARANTIA_ATUAL"],
        values=df["VALOR_CONSOLIDADO"],
        aggfunc="sum",
        margins=True
    )
    print("\nDIVIDA TOTAL (R$):")
    print(cross.map(lambda x: fmt_brl(x) if pd.notna(x) else "-"))

    cross_qtd = pd.crosstab(
        df["AJUIZADO_BIN"].map({True: "AJUIZADO", False: "NAO AJUIZADO"}),
        df["GARANTIA_ATUAL"],
        margins=True
    )
    print("\nQUANTIDADE DE INSCRICOES:")
    print(cross_qtd)

    # Mercado alvo
    print("\n" + "=" * 70)
    print("  MERCADO ALVO: AJUIZADO + SEM GARANTIA")
    print("=" * 70)

    mercado_alvo = df[(df["AJUIZADO_BIN"]) & (df["GARANTIA_ATUAL"] == "SEM GARANTIA")]

    print(f"\n  VOLUME:")
    print(f"    Inscricoes: {len(mercado_alvo):,}")
    print(f"    CNPJs unicos: {mercado_alvo['CPF_CNPJ'].nunique():,}")
    print(f"    Divida total: {fmt_brl(mercado_alvo['VALOR_CONSOLIDADO'].sum())}")
    print(f"    Divida media: {fmt_brl(mercado_alvo['VALOR_CONSOLIDADO'].mean())}")
    print(f"    Divida mediana: {fmt_brl(mercado_alvo['VALOR_CONSOLIDADO'].median())}")

    if len(mercado_alvo) > 0:
        print(f"\n  TOP 10 TRIBUTOS (AJUIZADO + SEM GARANTIA):")
        top_trib = mercado_alvo.groupby("RECEITA_PRINCIPAL").agg(
            DIVIDA=("VALOR_CONSOLIDADO", "sum")
        ).sort_values("DIVIDA", ascending=False).head(10)
        for idx, row in top_trib.iterrows():
            print(f"    {str(idx)[:50]}: {fmt_brl(row['DIVIDA'])}")

    print("\n" + "=" * 70)
    print("  ANALISE CONCLUIDA!")
    print("=" * 70)


if __name__ == "__main__":
    main()
