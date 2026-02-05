"""
Analise com corte temporal: safras 2020-2025.

Filtra ajuizados sem garantia, aplica corte temporal e segmentacao
por ticket, gerando base limpa de CNPJs qualificados.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
import sys

import config
from utils import (
    fmt_brl, pipeline_base, agregar_por_cnpj, calcular_potencial_comercial,
    salvar_csv, salvar_grafico
)


def carregar_todas_bases():
    """Carrega e consolida as 6 bases, retornando apenas ajuizados sem garantia."""
    df_all = []
    for arquivo, nome_curto, nome_completo in config.BASES:
        print(f"\n  Processando {nome_completo}...")
        try:
            df = pipeline_base(arquivo, colunas=config.COLUNAS_REDUZIDAS)
        except FileNotFoundError as e:
            print(f"    AVISO: {e}")
            continue

        # Filtrar ajuizados sem garantia
        df = df[(df["AJUIZADO_BIN"]) & (df["GARANTIA_ATUAL"] == "SEM GARANTIA")].copy()
        df["REGIAO"] = nome_completo
        df_all.append(df)
        print(f"    Total: {len(df):,} inscricoes | {df['CPF_CNPJ'].nunique():,} CNPJs")

    if not df_all:
        print("Nenhuma base carregada.")
        sys.exit(1)

    return pd.concat(df_all, ignore_index=True)


def main():
    print("=" * 100)
    print("  ANALISE COM CORTE TEMPORAL: 2020-2025 (ULTIMOS 5 ANOS)")
    print("=" * 100)

    df_brasil_completo = carregar_todas_bases()
    print(f"\n  Total Brasil (ANTES do corte): {len(df_brasil_completo):,} inscricoes | "
          f"{df_brasil_completo['CPF_CNPJ'].nunique():,} CNPJs")

    # Aplicar corte temporal
    print("\n" + "=" * 100)
    print("  APLICANDO CORTE: SAFRAS 2020-2025")
    print("=" * 100)

    df_brasil = df_brasil_completo[
        df_brasil_completo["SAFRA"].isin(config.SAFRAS_VALIDAS)
    ].copy()

    total_completo = len(df_brasil_completo)
    total_cortado = len(df_brasil)
    print(f"\n  Total Brasil (APOS corte 2020-2025):")
    print(f"    Inscricoes: {total_cortado:,} (de {total_completo:,} = {total_cortado/total_completo*100:.1f}%)")
    print(f"    CNPJs: {df_brasil['CPF_CNPJ'].nunique():,}")
    print(f"    Divida: {fmt_brl(df_brasil['VALOR_CONSOLIDADO'].sum())}")

    # Distribuicao por safra
    print("\n" + "=" * 100)
    print("  DISTRIBUICAO POR SAFRA (2020-2025)")
    print("=" * 100)

    safra_stats = df_brasil.groupby("SAFRA").agg(
        INSCRICOES=("NUMERO_INSCRICAO", "count"),
        CNPJs=("CPF_CNPJ", "nunique"),
        DIVIDA=("VALOR_CONSOLIDADO", "sum"),
        MEDIA=("VALOR_CONSOLIDADO", "mean"),
        MEDIANA=("VALOR_CONSOLIDADO", "median"),
        P25=("VALOR_CONSOLIDADO", lambda x: x.quantile(0.25)),
        P75=("VALOR_CONSOLIDADO", lambda x: x.quantile(0.75)),
    ).reindex(config.SAFRAS_VALIDAS)

    print(f"\n{'SAFRA':<12} {'INSCR':>10} {'CNPJs':>10} {'DIVIDA':>15} {'MEDIA':>12} {'MEDIANA':>12}")
    print("-" * 85)
    for safra in config.SAFRAS_VALIDAS:
        if safra in safra_stats.index:
            row = safra_stats.loc[safra]
            print(f"{safra:<12} {int(row['INSCRICOES']):>10,} {int(row['CNPJs']):>10,} "
                  f"{fmt_brl(row['DIVIDA']):>15} {fmt_brl(row['MEDIA']):>12} {fmt_brl(row['MEDIANA']):>12}")

    # Analise de ticket
    print("\n" + "=" * 100)
    print("  ANALISE DE TICKET (2020-2025)")
    print("=" * 100)

    print(f"\n{'FAIXA':<25} {'INSCRICOES':>12} {'%':>8} {'CNPJs':>10} {'DIVIDA':>18}")
    print("-" * 80)
    for nome, vmin, vmax in config.FAIXAS_TICKET:
        subset = df_brasil[
            (df_brasil["VALOR_CONSOLIDADO"] >= vmin) &
            (df_brasil["VALOR_CONSOLIDADO"] < vmax)
        ]
        pct = len(subset) / len(df_brasil) * 100 if len(df_brasil) > 0 else 0
        print(f"{nome:<25} {len(subset):>12,} {pct:>7.2f}% {subset['CPF_CNPJ'].nunique():>10,} "
              f"{fmt_brl(subset['VALOR_CONSOLIDADO'].sum()):>18}")

    # Classificacao por CNPJ
    print("\n" + "=" * 100)
    print("  LOGICA DE EXCLUSAO POR TICKET (R$ 1 MI)")
    print("=" * 100)

    cnpj_agg = agregar_por_cnpj(df_brasil)

    print("\n  CLASSIFICACAO DE CNPJs:")
    for classif in ["MANTER_INTEGRAL", "MANTER_FILTRAR", "EXCLUIR"]:
        subset = cnpj_agg[cnpj_agg["CLASSIFICACAO"] == classif]
        print(f"\n    {classif}:")
        print(f"      CNPJs: {len(subset):,}")
        print(f"      Inscricoes: {subset['TOTAL_INSCRICOES'].sum():,}")
        print(f"      Divida total: {fmt_brl(subset['DIVIDA_TOTAL'].sum())}")
        print(f"      Divida >= R$ 1mi: {fmt_brl(subset['DIVIDA_ACIMA_1MI'].sum())}")

    # Base final
    cnpjs_manter = cnpj_agg[cnpj_agg["CLASSIFICACAO"] != "EXCLUIR"]
    premio, comissao = calcular_potencial_comercial(cnpjs_manter["DIVIDA_ACIMA_1MI"].sum())

    print(f"\n  BASE FINAL:")
    print(f"    CNPJs: {len(cnpjs_manter):,}")
    print(f"    Divida relevante: {fmt_brl(cnpjs_manter['DIVIDA_ACIMA_1MI'].sum())}")
    print(f"    Premio estimado (2%): {fmt_brl(premio)}")
    print(f"    Comissao estimada (25%): {fmt_brl(comissao)}")

    # Graficos
    print("\n  Gerando graficos...")
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    cores_safras = config.CORES_SAFRAS

    # 1. Inscricoes por safra
    ax1 = axes[0, 0]
    safra_inscr = safra_stats["INSCRICOES"].dropna()
    bars1 = ax1.bar(safra_inscr.index, safra_inscr.values,
                    color=[cores_safras.get(s, '#95a5a6') for s in safra_inscr.index])
    ax1.set_title("Inscricoes por Safra (2020-2025)", fontweight='bold')
    ax1.set_ylabel("Quantidade")
    for bar, val in zip(bars1, safra_inscr.values):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{val:,.0f}',
                 ha='center', va='bottom', fontsize=9)

    # 2. Divida por safra
    ax2 = axes[0, 1]
    safra_divida = (safra_stats["DIVIDA"].dropna()) / 1e12
    bars2 = ax2.bar(safra_divida.index, safra_divida.values,
                    color=[cores_safras.get(s, '#95a5a6') for s in safra_divida.index])
    ax2.set_title("Divida por Safra (R$ trilhoes)", fontweight='bold')
    ax2.set_ylabel("R$ trilhoes")
    for bar, val in zip(bars2, safra_divida.values):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{val:.1f}',
                 ha='center', va='bottom', fontsize=9)

    # 3. Media e Mediana por safra
    ax3 = axes[0, 2]
    x = np.arange(len(config.SAFRAS_VALIDAS))
    width = 0.35
    ax3.bar(x - width/2, safra_stats["MEDIA"].fillna(0)/1e6, width, label='Media', color='#3498db')
    ax3.bar(x + width/2, safra_stats["MEDIANA"].fillna(0)/1e6, width, label='Mediana', color='#e74c3c')
    ax3.set_title("Media vs Mediana por Safra (R$ mi)", fontweight='bold')
    ax3.set_ylabel("R$ milhoes")
    ax3.set_xticks(x)
    ax3.set_xticklabels(config.SAFRAS_VALIDAS)
    ax3.legend()

    # 4. Histograma de valores (log)
    ax4 = axes[1, 0]
    valores_log = np.log10(df_brasil["VALOR_CONSOLIDADO"].clip(lower=1))
    ax4.hist(valores_log, bins=50, color='#3498db', alpha=0.7, edgecolor='white')
    ax4.axvline(np.log10(1_000_000), color='red', linestyle='--', linewidth=2, label='R$ 1 mi')
    ax4.axvline(np.log10(5_000_000), color='orange', linestyle='--', linewidth=2, label='R$ 5 mi')
    ax4.set_title("Distribuicao de Valores (log10) - 2020-2025", fontweight='bold')
    ax4.set_xlabel("log10(Valor)")
    ax4.set_ylabel("Frequencia")
    ax4.legend()

    # 5. CNPJs por regiao
    ax5 = axes[1, 1]
    regiao_cnpjs = df_brasil.groupby("REGIAO")["CPF_CNPJ"].nunique().sort_values(ascending=True)
    bars5 = ax5.barh(regiao_cnpjs.index, regiao_cnpjs.values, color='#27ae60')
    ax5.set_title("CNPJs por Regiao (2020-2025)", fontweight='bold')
    ax5.set_xlabel("CNPJs")

    # 6. Pizza classificacao
    ax6 = axes[1, 2]
    class_counts = cnpj_agg["CLASSIFICACAO"].value_counts()
    colors_class = [config.CORES_CLASSIFICACAO.get(c, '#95a5a6') for c in class_counts.index]
    ax6.pie(class_counts.values, labels=class_counts.index, autopct='%1.1f%%',
            colors=colors_class, startangle=90)
    ax6.set_title("Classificacao de CNPJs\n(Regra Ticket R$ 1 mi)", fontweight='bold')

    plt.suptitle("ANALISE BASE LIMPA - SAFRAS 2020-2025", fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    salvar_grafico(fig, "analise_2020_2025_base_limpa.png")
    plt.close(fig)

    # Exportar CSVs
    salvar_csv(cnpj_agg, "cnpjs_2020_2025_classificados.csv")
    salvar_csv(cnpjs_manter, "cnpjs_2020_2025_base_final.csv")

    df_final = df_brasil[
        (df_brasil["CPF_CNPJ"].isin(cnpjs_manter["CPF_CNPJ"])) &
        (df_brasil["VALOR_CONSOLIDADO"] >= config.TICKET_MINIMO)
    ].copy()
    salvar_csv(df_final, "inscricoes_2020_2025_base_final.csv")

    print("\n" + "=" * 100)
    print("  ANALISE 2020-2025 CONCLUIDA!")
    print("=" * 100)


if __name__ == "__main__":
    main()
