"""
Analise de garantias em inscricoes ajuizadas.

Foca na base SIDA_1 para analise detalhada de distribuicao de garantias,
percentis e comparativo media vs mediana por tipo de garantia.
"""

import pandas as pd
import sys

import config
from utils import fmt_brl, pipeline_base


ORDEM_GARANTIA = ["SEGURO GARANTIA", "CARTA FIANCA", "DEPOSITO", "PENHORA", "SEM GARANTIA"]


def main():
    print("=" * 80)
    print("  LENDO BASE PGFN COMPLETA...")
    print("=" * 80)

    # Usa a primeira base por padrao
    arquivo = config.BASES[0][0]
    try:
        df = pipeline_base(arquivo, colunas=config.COLUNAS_COMPLETAS)
    except FileNotFoundError as e:
        print(f"  ERRO: {e}")
        sys.exit(1)

    # Somente ajuizados
    df_ajuizado = df[df["AJUIZADO_BIN"]].copy()
    print(f"\n  Total AJUIZADOS: {len(df_ajuizado):,} registros")

    # Estatisticas por tipo de garantia
    print("\n" + "=" * 100)
    print("  AJUIZADOS - ESTATISTICAS POR TIPO DE GARANTIA")
    print("=" * 100)

    print(f"\n{'GARANTIA':<20} {'QTD':>12} {'CNPJs':>10} {'DIVIDA TOTAL':>25} {'MEDIA':>20} {'MEDIANA':>20}")
    print("-" * 110)

    resultados = []
    for garantia in ORDEM_GARANTIA:
        subset = df_ajuizado[df_ajuizado["GARANTIA_ATUAL"] == garantia]
        if len(subset) > 0:
            qtd = len(subset)
            cnpjs = subset["CPF_CNPJ"].nunique()
            divida_total = subset["VALOR_CONSOLIDADO"].sum()
            media = subset["VALOR_CONSOLIDADO"].mean()
            mediana = subset["VALOR_CONSOLIDADO"].median()

            resultados.append({
                "GARANTIA": garantia, "QTD": qtd, "CNPJs": cnpjs,
                "DIVIDA_TOTAL": divida_total, "MEDIA": media, "MEDIANA": mediana,
            })
            print(f"{garantia:<20} {qtd:>12,} {cnpjs:>10,} {fmt_brl(divida_total):>25} "
                  f"{fmt_brl(media):>20} {fmt_brl(mediana):>20}")
        else:
            print(f"{garantia:<20} {'0':>12} {'-':>10} {'-':>25} {'-':>20} {'-':>20}")

    print("-" * 110)
    total_qtd = len(df_ajuizado)
    total_cnpjs = df_ajuizado["CPF_CNPJ"].nunique()
    total_divida = df_ajuizado["VALOR_CONSOLIDADO"].sum()
    print(f"{'TOTAL AJUIZADOS':<20} {total_qtd:>12,} {total_cnpjs:>10,} {fmt_brl(total_divida):>25} "
          f"{fmt_brl(df_ajuizado['VALOR_CONSOLIDADO'].mean()):>20} "
          f"{fmt_brl(df_ajuizado['VALOR_CONSOLIDADO'].median()):>20}")

    # Comparativo media vs mediana
    print("\n" + "=" * 100)
    print("  COMPARATIVO - MEDIA vs MEDIANA (AJUIZADOS)")
    print("=" * 100)
    print(f"\n{'GARANTIA':<20} {'MEDIA':>25} {'MEDIANA':>25} {'RATIO M/m':>15}")
    print("-" * 90)
    for r in resultados:
        ratio = r["MEDIA"] / r["MEDIANA"] if r["MEDIANA"] > 0 else 0
        print(f"{r['GARANTIA']:<20} {fmt_brl(r['MEDIA']):>25} {fmt_brl(r['MEDIANA']):>25} {ratio:>15.1f}x")

    # Percentis
    print("\n" + "=" * 100)
    print("  PERCENTIS DE VALOR (AJUIZADOS) - P25, P50, P75, P90, P95")
    print("=" * 100)
    print(f"\n{'GARANTIA':<20} {'P25':>18} {'P50 (MED)':>18} {'P75':>18} {'P90':>18} {'P95':>18}")
    print("-" * 115)

    for garantia in ORDEM_GARANTIA:
        subset = df_ajuizado[df_ajuizado["GARANTIA_ATUAL"] == garantia]["VALOR_CONSOLIDADO"]
        if len(subset) > 0:
            p25 = subset.quantile(0.25)
            p50 = subset.quantile(0.50)
            p75 = subset.quantile(0.75)
            p90 = subset.quantile(0.90)
            p95 = subset.quantile(0.95)
            print(f"{garantia:<20} {fmt_brl(p25):>18} {fmt_brl(p50):>18} {fmt_brl(p75):>18} "
                  f"{fmt_brl(p90):>18} {fmt_brl(p95):>18}")

    print("\n" + "=" * 100)
    print("  ANALISE CONCLUIDA!")
    print("=" * 100)


if __name__ == "__main__":
    main()
