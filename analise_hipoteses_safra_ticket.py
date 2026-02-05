"""
Analise de hipoteses: distribuicao temporal por regiao e impacto do ticket.

Testa 4 hipoteses:
1. Distribuicao temporal e congruente entre regioes? (Chi-quadrado)
2. Impacto do corte de ticket (R$ 1 MI)
3. Logica de exclusao de CNPJ por ticket
4. Interacao safra x ticket
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
import sys

import config
from utils import (
    fmt_brl, pipeline_base, agregar_por_cnpj, salvar_csv, salvar_grafico
)


def carregar_bases_ajuizados_sem_garantia():
    """Carrega todas as bases, filtrando ajuizados sem garantia."""
    df_all = []
    for arquivo, nome_curto, nome_completo in config.BASES:
        print(f"\n  Processando {nome_completo}...")
        try:
            df = pipeline_base(arquivo, colunas=config.COLUNAS_REDUZIDAS)
        except FileNotFoundError as e:
            print(f"    AVISO: {e}")
            continue

        df = df[(df["AJUIZADO_BIN"]) & (df["GARANTIA_ATUAL"] == "SEM GARANTIA")].copy()
        df["REGIAO"] = nome_completo
        df_all.append(df)
        print(f"    {len(df):,} inscricoes | {df['CPF_CNPJ'].nunique():,} CNPJs")

    if not df_all:
        print("Nenhuma base carregada.")
        sys.exit(1)

    return pd.concat(df_all, ignore_index=True)


def main():
    print("=" * 100)
    print("  ANALISE DE DISTRIBUICAO TEMPORAL POR REGIAO")
    print("=" * 100)

    df_brasil = carregar_bases_ajuizados_sem_garantia()
    print(f"\n  Total Brasil: {len(df_brasil):,} inscricoes | {df_brasil['CPF_CNPJ'].nunique():,} CNPJs")

    # ========== HIPOTESE 1: Distribuicao temporal por regiao ==========
    print("\n" + "=" * 100)
    print("  HIPOTESE 1: DISTRIBUICAO TEMPORAL E CONGRUENTE ENTRE REGIOES?")
    print("=" * 100)

    ordem_safras_full = ["2024-2025", "2023", "2022", "2021", "2020", "2019", "<=2018", "SEM DATA"]
    cross_abs_raw = pd.crosstab(df_brasil["REGIAO"], df_brasil["SAFRA"])
    ordem_safras = [s for s in ordem_safras_full if s in cross_abs_raw.columns]
    cross_abs = cross_abs_raw[ordem_safras]

    print("\n  CONTAGEM ABSOLUTA:")
    print(cross_abs.to_string())

    cross_pct_raw = pd.crosstab(df_brasil["REGIAO"], df_brasil["SAFRA"], normalize='index') * 100
    cross_pct = cross_pct_raw[[s for s in ordem_safras if s in cross_pct_raw.columns]]
    print("\n  PERCENTUAL POR REGIAO:")
    print(cross_pct.round(2).to_string())

    # Teste Chi-quadrado
    chi2, p_value, dof, expected = stats.chi2_contingency(cross_abs)
    n = cross_abs.sum().sum()
    min_dim = min(cross_abs.shape) - 1
    cramers_v = np.sqrt(chi2 / (n * min_dim)) if min_dim > 0 else 0

    print(f"\n  TESTE CHI-QUADRADO:")
    print(f"    Chi2 = {chi2:,.2f}")
    print(f"    p-valor = {p_value:.2e}")
    print(f"    Graus de liberdade = {dof}")
    print(f"    -> {'DISTRIBUICOES DIFERENTES' if p_value < 0.05 else 'DISTRIBUICOES SIMILARES'} (alfa=0.05)")
    print(f"    Cramer's V = {cramers_v:.4f} ({'fraco' if cramers_v < 0.1 else 'moderado' if cramers_v < 0.3 else 'forte'})")

    # ========== HIPOTESE 2: Impacto do corte de ticket ==========
    print("\n" + "=" * 100)
    print("  HIPOTESE 2: IMPACTO DO CORTE DE TICKET (R$ 1 MI)")
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

    # ========== HIPOTESE 3: Logica de exclusao ==========
    print("\n" + "=" * 100)
    print("  HIPOTESE 3: LOGICA DE EXCLUSAO POR TICKET")
    print("=" * 100)

    cnpj_agg = agregar_por_cnpj(df_brasil)

    print("\n  CLASSIFICACAO DE CNPJs:")
    class_resumo = cnpj_agg.groupby("CLASSIFICACAO").agg(
        CNPJs=("CPF_CNPJ", "count"),
        INSCR_TOTAL=("TOTAL_INSCRICOES", "sum"),
        DIVIDA_TOTAL=("DIVIDA_TOTAL", "sum"),
        DIVIDA_ACIMA_1MI=("DIVIDA_ACIMA_1MI", "sum"),
    ).reset_index()

    for _, row in class_resumo.iterrows():
        print(f"\n    {row['CLASSIFICACAO']}:")
        print(f"      CNPJs: {row['CNPJs']:,}")
        print(f"      Inscricoes: {row['INSCR_TOTAL']:,}")
        print(f"      Divida total: {fmt_brl(row['DIVIDA_TOTAL'])}")

    cnpjs_manter = cnpj_agg[cnpj_agg["CLASSIFICACAO"] != "EXCLUIR"]
    cnpjs_excluir = cnpj_agg[cnpj_agg["CLASSIFICACAO"] == "EXCLUIR"]
    print(f"\n  IMPACTO DA REGRA DE TICKET:")
    print(f"    CNPJs ANTES: {len(cnpj_agg):,}")
    print(f"    CNPJs DEPOIS: {len(cnpjs_manter):,} (-{len(cnpjs_excluir):,})")
    print(f"    Divida relevante: {fmt_brl(cnpjs_manter['DIVIDA_ACIMA_1MI'].sum())}")

    # ========== HIPOTESE 4: Safra x Ticket ==========
    print("\n" + "=" * 100)
    print("  HIPOTESE 4: INTERACAO SAFRA x TICKET")
    print("=" * 100)

    df_filtrado = df_brasil[df_brasil["VALOR_CONSOLIDADO"] >= config.TICKET_MINIMO].copy()
    print(f"\n  Apos filtro >= R$ 1 mi:")
    print(f"    Inscricoes: {len(df_filtrado):,} (de {len(df_brasil):,} = "
          f"{len(df_filtrado)/len(df_brasil)*100:.1f}%)")
    print(f"    CNPJs: {df_filtrado['CPF_CNPJ'].nunique():,}")
    print(f"    Divida: {fmt_brl(df_filtrado['VALOR_CONSOLIDADO'].sum())}")

    safra_filtrado = df_filtrado.groupby("SAFRA").agg(
        INSCRICOES=("NUMERO_INSCRICAO", "count"),
        CNPJs=("CPF_CNPJ", "nunique"),
        DIVIDA=("VALOR_CONSOLIDADO", "sum"),
        MEDIA=("VALOR_CONSOLIDADO", "mean"),
        MEDIANA=("VALOR_CONSOLIDADO", "median"),
    ).reindex([s for s in ordem_safras if s != "SEM DATA"])

    print(f"\n{'SAFRA':<12} {'INSCR':>10} {'CNPJs':>10} {'DIVIDA':>18} {'MEDIA':>15} {'MEDIANA':>15}")
    print("-" * 85)
    for safra in [s for s in ordem_safras if s != "SEM DATA"]:
        if safra in safra_filtrado.index and not pd.isna(safra_filtrado.loc[safra, "INSCRICOES"]):
            row = safra_filtrado.loc[safra]
            print(f"{safra:<12} {int(row['INSCRICOES']):>10,} {int(row['CNPJs']):>10,} "
                  f"{fmt_brl(row['DIVIDA']):>18} {fmt_brl(row['MEDIA']):>15} {fmt_brl(row['MEDIANA']):>15}")

    # ========== GRAFICOS ==========
    print("\n  Gerando graficos...")
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    cores_safras = config.CORES_SAFRAS

    # 1. Distribuicao % por regiao (stacked bar)
    ax1 = axes[0, 0]
    safras_plot = [s for s in ["2024-2025", "2023", "2022", "2021", "2020", "2019", "<=2018"]
                   if s in cross_pct.columns]
    if safras_plot:
        cross_pct[safras_plot].plot(
            kind='bar', stacked=True, ax=ax1,
            color=[cores_safras.get(s, '#95a5a6') for s in safras_plot]
        )
    ax1.set_title("Distribuicao % por Safra (cada regiao = 100%)", fontweight='bold')
    ax1.set_ylabel("Percentual")
    ax1.set_xlabel("")
    ax1.legend(title="Safra", bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=8)
    ax1.tick_params(axis='x', rotation=45)

    # 2. Boxplot idade por regiao
    ax2 = axes[0, 1]
    if "IDADE_ANOS" in df_brasil.columns:
        df_brasil.boxplot(column="IDADE_ANOS", by="REGIAO", ax=ax2)
    ax2.set_title("Distribuicao de Idade por Regiao", fontweight='bold')
    ax2.set_xlabel("")
    ax2.set_ylabel("Idade (anos)")
    plt.suptitle("")
    ax2.tick_params(axis='x', rotation=45)

    # 3. Histograma de valores (log)
    ax3 = axes[0, 2]
    valores_log = np.log10(df_brasil["VALOR_CONSOLIDADO"].clip(lower=1))
    ax3.hist(valores_log, bins=50, color='#3498db', alpha=0.7, edgecolor='white')
    ax3.axvline(np.log10(1_000_000), color='red', linestyle='--', linewidth=2, label='R$ 1 mi')
    ax3.axvline(np.log10(5_000_000), color='orange', linestyle='--', linewidth=2, label='R$ 5 mi')
    ax3.set_title("Distribuicao de Valores (log10)", fontweight='bold')
    ax3.set_xlabel("log10(Valor)")
    ax3.set_ylabel("Frequencia")
    ax3.legend()

    # 4. Safra x Valor medio
    ax4 = axes[1, 0]
    safras_media_list = [s for s in ["2024-2025", "2023", "2022", "2021", "2020", "2019", "<=2018"]
                         if s in df_brasil["SAFRA"].unique()]
    safra_media = df_brasil.groupby("SAFRA")["VALOR_CONSOLIDADO"].mean().reindex(safras_media_list) / 1e6
    if not safra_media.empty:
        bars = ax4.bar(safra_media.index, safra_media.values,
                       color=[cores_safras.get(s, '#95a5a6') for s in safra_media.index])
        for bar, val in zip(bars, safra_media.values):
            ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{val:.1f}',
                     ha='center', va='bottom', fontsize=9)
    ax4.set_title("Valor Medio por Safra (R$ milhoes)", fontweight='bold')
    ax4.set_ylabel("R$ milhoes")
    ax4.tick_params(axis='x', rotation=45)

    # 5. CNPJs por classificacao de ticket
    ax5 = axes[1, 1]
    class_counts = cnpj_agg["CLASSIFICACAO"].value_counts()
    colors_class = [config.CORES_CLASSIFICACAO.get(c, '#95a5a6') for c in class_counts.index]
    ax5.pie(class_counts.values, labels=None, autopct='%1.1f%%',
            colors=colors_class, startangle=90)
    ax5.legend(class_counts.index, loc='center left', bbox_to_anchor=(1, 0.5), fontsize=9)
    ax5.set_title("Classificacao de CNPJs\n(Regra Ticket R$ 1 mi)", fontweight='bold')

    # 6. Volume por safra (filtrado >= R$ 1mi)
    ax6 = axes[1, 2]
    if not safra_filtrado.empty:
        safra_vol = safra_filtrado["DIVIDA"].dropna() / 1e12
        if not safra_vol.empty:
            bars = ax6.bar(safra_vol.index, safra_vol.values,
                           color=[cores_safras.get(s, '#95a5a6') for s in safra_vol.index])
            for bar, val in zip(bars, safra_vol.values):
                ax6.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{val:.1f}',
                         ha='center', va='bottom', fontsize=9)
    ax6.set_title("Divida por Safra (>= R$ 1 mi, R$ tri)", fontweight='bold')
    ax6.set_ylabel("R$ trilhoes")
    ax6.tick_params(axis='x', rotation=45)

    plt.suptitle("ANALISE DE HIPOTESES - DISTRIBUICAO TEMPORAL E TICKET",
                 fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    salvar_grafico(fig, "analise_hipoteses_safra_ticket.png")
    plt.close(fig)

    # Exportar
    salvar_csv(cnpj_agg, "cnpjs_classificados_por_ticket.csv")
    salvar_csv(cross_pct, "distribuicao_safra_por_regiao_pct.csv")

    print("\n" + "=" * 100)
    print("  ANALISE DE HIPOTESES CONCLUIDA!")
    print("=" * 100)


if __name__ == "__main__":
    main()
