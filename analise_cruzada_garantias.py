"""
Analise Cruzada: Garantias no Tempo + RJ/Falencia + Comparativo de Tipos

Base: pgfn_unificada_cnpj_principal_semibruta.csv

Analises:
1) Distribuicao temporal de garantias (anual, mensal, diaria)
2) Identificacao de empresas em Recuperacao Judicial / Falencia
3) Comparativo evolutivo entre tipos de garantia (SG vs Penhora vs outros)

Saida: outputs_analise_cruzada/
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path
import sys
import re

import config
from utils import fmt_brl, converter_valor_brl, extrair_garantia

# Diretorios
OUTPUT_DIR = config.DATA_DIR / "outputs_analise_cruzada"


def setup():
    """Configura diretorio de saida."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"  Diretorio de saida: {OUTPUT_DIR}")


def salvar_csv(df, nome):
    """Salva CSV no diretorio de saida."""
    path = OUTPUT_DIR / nome
    df.to_csv(path, sep=";", index=False, encoding="utf-8-sig")
    print(f"    CSV: {path.name}")
    return path


def salvar_fig(fig, nome, dpi=150):
    """Salva figura no diretorio de saida."""
    path = OUTPUT_DIR / nome
    fig.savefig(path, dpi=dpi, bbox_inches="tight")
    print(f"    PNG: {path.name}")
    plt.close(fig)
    return path


def carregar_base_unificada():
    """Carrega a base unificada."""
    arquivo = config.get_data_path("pgfn_unificada_cnpj_principal_semibruta.csv")

    print(f"\n  Lendo: {arquivo}")

    if not arquivo.exists():
        print(f"  ERRO: Arquivo nao encontrado!")
        print(f"  Execute primeiro: python unificar_bases_pgfn.py")
        sys.exit(1)

    df = pd.read_csv(arquivo, sep=";", encoding="utf-8-sig", low_memory=False)
    print(f"  Registros: {len(df):,}")

    return df


def preparar_dados(df):
    """Prepara dados: converte datas, valores, extrai garantia."""
    df = df.copy()

    # Converter valor (pode estar como string)
    if df["VALOR_CONSOLIDADO_NUM"].dtype == object:
        df["VALOR_CONSOLIDADO_NUM"] = pd.to_numeric(
            df["VALOR_CONSOLIDADO_NUM"].astype(str).str.replace(",", "."),
            errors="coerce"
        )

    # Converter data
    df["DATA_DT"] = pd.to_datetime(df["DATA_INSCRICAO"], errors="coerce", dayfirst=True)
    df["ANO"] = df["DATA_DT"].dt.year
    df["MES"] = df["DATA_DT"].dt.to_period("M")
    df["DATA"] = df["DATA_DT"].dt.date

    # Extrair tipo de garantia
    df["GARANTIA"] = df["SITUACAO_INSCRICAO"].apply(extrair_garantia)

    # Identificar RJ/Falencia pelo NOME_DEVEDOR
    df["NOME_UPPER"] = df["NOME_DEVEDOR"].astype(str).str.upper().str.strip()

    # Padroes para Recuperacao Judicial e Falencia
    # Cuidado: "RJ" pode ser estado ou "Recuperacao Judicial"
    # Identificar pelo contexto: no inicio/fim do nome, com separadores
    patterns_rj = [
        r'^EM\s+RECUPERA[CÇ][AÃ]O\s+JUDICIAL',
        r'\s+EM\s+RECUPERA[CÇ][AÃ]O\s+JUDICIAL$',
        r'^EM\s+REC\.?\s*JUD',
        r'\s+EM\s+REC\.?\s*JUD\.?$',
        r'^RECUPERA[CÇ][AÃ]O\s+JUDICIAL',
        r'\s+RECUPERA[CÇ][AÃ]O\s+JUDICIAL$',
        r'\s+-\s*RJ$',  # " - RJ" no final (mas nao se for UF)
        r'^\s*RJ\s+-',  # "RJ -" no inicio
        r'\(EM\s+RECUPERA[CÇ][AÃ]O',
        r'RECUPERA[CÇ][AÃ]O\s+JUDICIAL',
    ]

    patterns_falencia = [
        r'^EM\s+FAL[EÊ]NCIA',
        r'\s+EM\s+FAL[EÊ]NCIA$',
        r'^FAL[EÊ]NCIA',
        r'\s+FAL[EÊ]NCIA$',
        r'^MASSA\s+FALIDA',
        r'\s+MASSA\s+FALIDA',
        r'FALIDO',
        r'FALIDA',
    ]

    def detectar_rj(nome, uf):
        """Detecta Recuperacao Judicial no nome, evitando confusao com UF RJ."""
        nome_str = str(nome).upper()
        uf_str = str(uf).upper().strip()

        # Se termina com " - RJ" mas UF e RJ, provavelmente e estado
        if nome_str.endswith(" - RJ") and uf_str == "RJ":
            # Verificar se tem outros indicios de RJ
            if "RECUPERA" in nome_str or "REC. JUD" in nome_str.replace(".", ""):
                return True
            return False

        # Verificar padroes
        for pattern in patterns_rj:
            if re.search(pattern, nome_str):
                return True
        return False

    def detectar_falencia(nome):
        """Detecta Falencia/Massa Falida no nome."""
        nome_str = str(nome).upper()
        for pattern in patterns_falencia:
            if re.search(pattern, nome_str):
                return True
        return False

    df["FLAG_RECUPERACAO_JUDICIAL"] = df.apply(
        lambda row: detectar_rj(row["NOME_UPPER"], row["UF_DEVEDOR"]), axis=1
    )
    df["FLAG_FALENCIA"] = df["NOME_UPPER"].apply(detectar_falencia)
    df["FLAG_RJ_OU_FALENCIA"] = df["FLAG_RECUPERACAO_JUDICIAL"] | df["FLAG_FALENCIA"]

    # Remover dados sem data valida para analises temporais
    df_valido = df[df["DATA_DT"].notna()].copy()
    print(f"  Registros com data valida: {len(df_valido):,}")

    return df_valido


# =============================================================================
# ANALISE 1: DISTRIBUICAO TEMPORAL
# =============================================================================

def analise_temporal_anual(df):
    """Histograma anual de volume e divida."""
    print("\n" + "-" * 80)
    print("  ANALISE 1A: DISTRIBUICAO ANUAL")
    print("-" * 80)

    anual = df.groupby("ANO").agg(
        N_INSCRICOES=("NUMERO_INSCRICAO", "count"),
        VALOR_TOTAL=("VALOR_CONSOLIDADO_NUM", "sum"),
        CNPJS=("CPF_CNPJ", "nunique"),
    ).reset_index()

    anual = anual[anual["ANO"].notna() & (anual["ANO"] >= 1990) & (anual["ANO"] <= 2026)]
    anual["ANO"] = anual["ANO"].astype(int)
    anual = anual.sort_values("ANO")

    salvar_csv(anual, "temporal_anual.csv")

    # Grafico: barras duplas (volume e valor)
    fig, ax1 = plt.subplots(figsize=(16, 8))

    x = range(len(anual))
    width = 0.4

    bars = ax1.bar(x, anual["N_INSCRICOES"], width, color="#3498db", alpha=0.7, label="Inscricoes")
    ax1.set_xlabel("Ano", fontsize=11)
    ax1.set_ylabel("Quantidade de Inscricoes", color="#3498db", fontsize=11)
    ax1.tick_params(axis="y", labelcolor="#3498db")
    ax1.set_xticks(x)
    ax1.set_xticklabels(anual["ANO"], rotation=45, ha="right")

    ax2 = ax1.twinx()
    ax2.plot(x, anual["VALOR_TOTAL"] / 1e9, color="#e74c3c", linewidth=2.5,
             marker="o", markersize=6, label="Valor Total (R$ bi)")
    ax2.set_ylabel("Valor Total (R$ bilhoes)", color="#e74c3c", fontsize=11)
    ax2.tick_params(axis="y", labelcolor="#e74c3c")

    ax1.set_title("Distribuicao Anual - Base Unificada PGFN (PJ + Principal)\n"
                  "Barras = Inscricoes | Linha = Valor Total",
                  fontsize=13, fontweight="bold")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    ax1.grid(axis="y", alpha=0.3, linestyle="--")
    plt.tight_layout()
    salvar_fig(fig, "temporal_anual.png")

    # Print resumo
    print(f"\n  ANO       INSCR        VALOR TOTAL")
    print("  " + "-" * 40)
    for _, row in anual.tail(15).iterrows():
        print(f"  {int(row['ANO'])}    {row['N_INSCRICOES']:>8,}    {fmt_brl(row['VALOR_TOTAL'])}")

    return anual


def analise_temporal_mensal_60m(df):
    """Serie mensal dos ultimos 60 meses com media movel."""
    print("\n" + "-" * 80)
    print("  ANALISE 1B: SERIE MENSAL (ULTIMOS 60 MESES)")
    print("-" * 80)

    mensal = df.groupby("MES").agg(
        N_INSCRICOES=("NUMERO_INSCRICAO", "count"),
        VALOR_TOTAL=("VALOR_CONSOLIDADO_NUM", "sum"),
    ).reset_index()

    mensal = mensal.sort_values("MES")
    mensal["DATA_MES"] = mensal["MES"].dt.to_timestamp()

    # Ultimos 60 meses
    ultimos_60 = mensal.tail(60).copy()

    # Media movel 3 e 12 meses
    ultimos_60["MM3"] = ultimos_60["N_INSCRICOES"].rolling(3, min_periods=1).mean()
    ultimos_60["MM12"] = ultimos_60["N_INSCRICOES"].rolling(12, min_periods=1).mean()

    salvar_csv(ultimos_60, "temporal_mensal_60m.csv")

    # Grafico
    fig, ax1 = plt.subplots(figsize=(16, 8))

    ax1.bar(ultimos_60["DATA_MES"], ultimos_60["N_INSCRICOES"],
            width=20, color="#3498db", alpha=0.5, label="Inscricoes")
    ax1.plot(ultimos_60["DATA_MES"], ultimos_60["MM3"],
             color="#2980b9", linewidth=2, linestyle="--", label="MM3")
    ax1.plot(ultimos_60["DATA_MES"], ultimos_60["MM12"],
             color="#1a5276", linewidth=2.5, label="MM12")

    ax1.set_xlabel("Mes", fontsize=11)
    ax1.set_ylabel("Quantidade de Inscricoes", fontsize=11)
    ax1.xaxis.set_major_locator(mdates.YearLocator())
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax1.legend(loc="upper left")

    ax1.set_title("Serie Mensal - Ultimos 60 Meses\nBarras = Volume | Linhas = Media Movel",
                  fontsize=13, fontweight="bold")

    ax1.grid(axis="y", alpha=0.3, linestyle="--")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    salvar_fig(fig, "temporal_mensal_60m.png")

    return ultimos_60


def analise_temporal_diaria(df):
    """Dispersao diaria."""
    print("\n" + "-" * 80)
    print("  ANALISE 1C: DISTRIBUICAO DIARIA")
    print("-" * 80)

    diaria = df.groupby("DATA").agg(
        N_INSCRICOES=("NUMERO_INSCRICAO", "count"),
        VALOR_TOTAL=("VALOR_CONSOLIDADO_NUM", "sum"),
    ).reset_index()

    diaria["DATA"] = pd.to_datetime(diaria["DATA"])
    diaria = diaria.sort_values("DATA")

    # Ultimos 5 anos para visualizacao mais clara
    data_corte = diaria["DATA"].max() - pd.DateOffset(years=5)
    diaria_recente = diaria[diaria["DATA"] >= data_corte].copy()

    salvar_csv(diaria_recente, "temporal_diaria_5anos.csv")

    # Grafico scatter
    fig, ax = plt.subplots(figsize=(16, 7))

    sizes = (diaria_recente["N_INSCRICOES"] / diaria_recente["N_INSCRICOES"].max() * 100 + 5)
    scatter = ax.scatter(diaria_recente["DATA"], diaria_recente["N_INSCRICOES"],
                         s=sizes, c="#3498db", alpha=0.4, edgecolors="none")

    ax.set_xlabel("Data", fontsize=11)
    ax.set_ylabel("Inscricoes por Dia", fontsize=11)
    ax.set_title("Dispersao Diaria - Ultimos 5 Anos", fontsize=13, fontweight="bold")

    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.grid(axis="both", alpha=0.3, linestyle="--")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    salvar_fig(fig, "temporal_diaria_5anos.png")

    return diaria


# =============================================================================
# ANALISE 2: RECUPERACAO JUDICIAL E FALENCIA
# =============================================================================

def analise_rj_falencia(df):
    """Analise de empresas em RJ ou Falencia."""
    print("\n" + "-" * 80)
    print("  ANALISE 2: RECUPERACAO JUDICIAL E FALENCIA")
    print("-" * 80)

    # Contagens
    total = len(df)
    n_rj = df["FLAG_RECUPERACAO_JUDICIAL"].sum()
    n_falencia = df["FLAG_FALENCIA"].sum()
    n_rj_ou_falencia = df["FLAG_RJ_OU_FALENCIA"].sum()

    print(f"\n  VISAO GERAL:")
    print(f"    Total inscricoes: {total:,}")
    print(f"    Em Recuperacao Judicial: {n_rj:,} ({n_rj/total*100:.2f}%)")
    print(f"    Em Falencia/Massa Falida: {n_falencia:,} ({n_falencia/total*100:.2f}%)")
    print(f"    RJ ou Falencia (uniao): {n_rj_ou_falencia:,} ({n_rj_ou_falencia/total*100:.2f}%)")

    # Separar os grupos
    df_rj = df[df["FLAG_RJ_OU_FALENCIA"]].copy()
    df_normal = df[~df["FLAG_RJ_OU_FALENCIA"]].copy()

    # Metricas por grupo
    print(f"\n  METRICAS POR STATUS:")
    for nome, subset in [("Em RJ/Falencia", df_rj), ("Normal", df_normal)]:
        if len(subset) == 0:
            continue
        print(f"\n    {nome}:")
        print(f"      Inscricoes: {len(subset):,}")
        print(f"      CNPJs unicos: {subset['CPF_CNPJ'].nunique():,}")
        print(f"      Valor total: {fmt_brl(subset['VALOR_CONSOLIDADO_NUM'].sum())}")
        print(f"      Ticket medio: {fmt_brl(subset['VALOR_CONSOLIDADO_NUM'].mean())}")
        print(f"      Ticket mediano: {fmt_brl(subset['VALOR_CONSOLIDADO_NUM'].median())}")

    # Cruzamento RJ/Falencia x Garantia
    print(f"\n  CRUZAMENTO RJ/FALENCIA x TIPO DE GARANTIA:")
    cross = df.groupby(["FLAG_RJ_OU_FALENCIA", "GARANTIA"]).agg(
        INSCRICOES=("NUMERO_INSCRICAO", "count"),
        VALOR=("VALOR_CONSOLIDADO_NUM", "sum"),
    ).reset_index()

    cross_pivot = cross.pivot(index="GARANTIA", columns="FLAG_RJ_OU_FALENCIA", values="INSCRICOES").fillna(0)
    cross_pivot.columns = ["Normal", "RJ/Falencia"]
    cross_pivot["TOTAL"] = cross_pivot.sum(axis=1)
    cross_pivot["%_RJ_FALENCIA"] = cross_pivot["RJ/Falencia"] / cross_pivot["TOTAL"] * 100
    cross_pivot = cross_pivot.sort_values("TOTAL", ascending=False)

    print(f"\n  {'GARANTIA':<20} {'NORMAL':>10} {'RJ/FAL':>10} {'%RJ/FAL':>10}")
    print("  " + "-" * 55)
    for garantia, row in cross_pivot.iterrows():
        print(f"  {garantia:<20} {int(row['Normal']):>10,} {int(row['RJ/Falencia']):>10,} "
              f"{row['%_RJ_FALENCIA']:>9.2f}%")

    # Seguro Garantia especificamente
    print(f"\n  SEGURO GARANTIA EM RJ/FALENCIA:")
    sg = df[df["GARANTIA"] == "SEGURO GARANTIA"]
    sg_rj = sg[sg["FLAG_RJ_OU_FALENCIA"]]
    print(f"    Total SG: {len(sg):,}")
    print(f"    SG em RJ/Falencia: {len(sg_rj):,} ({len(sg_rj)/len(sg)*100:.2f}%)")
    print(f"    Valor SG em RJ/Falencia: {fmt_brl(sg_rj['VALOR_CONSOLIDADO_NUM'].sum())}")

    # Exportar lista de empresas em RJ/Falencia com Seguro Garantia
    if len(sg_rj) > 0:
        sg_rj_export = sg_rj.groupby("CPF_CNPJ").agg(
            NOME=("NOME_DEVEDOR", "first"),
            UF=("UF_DEVEDOR", "first"),
            N_INSCRICOES=("NUMERO_INSCRICAO", "count"),
            VALOR_TOTAL=("VALOR_CONSOLIDADO_NUM", "sum"),
            RJ=("FLAG_RECUPERACAO_JUDICIAL", "any"),
            FALENCIA=("FLAG_FALENCIA", "any"),
        ).reset_index().sort_values("VALOR_TOTAL", ascending=False)

        salvar_csv(sg_rj_export, "sg_em_rj_falencia.csv")

    # Grafico: comparativo
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))

    # Pie chart geral
    sizes = [total - n_rj_ou_falencia, n_rj_ou_falencia]
    labels = ["Normal", "RJ/Falencia"]
    colors = ["#27ae60", "#e74c3c"]
    ax1.pie(sizes, labels=labels, autopct="%1.1f%%", colors=colors, startangle=90,
            explode=(0, 0.1))
    ax1.set_title("Distribuicao Geral\nNormal vs RJ/Falencia", fontweight="bold")

    # Barras por tipo de garantia
    garantias = cross_pivot.index.tolist()
    x = range(len(garantias))
    width = 0.35

    ax2.bar([i - width/2 for i in x], cross_pivot["Normal"],
            width, label="Normal", color="#27ae60", alpha=0.7)
    ax2.bar([i + width/2 for i in x], cross_pivot["RJ/Falencia"],
            width, label="RJ/Falencia", color="#e74c3c", alpha=0.7)

    ax2.set_xticks(x)
    ax2.set_xticklabels([g[:15] for g in garantias], rotation=45, ha="right")
    ax2.set_ylabel("Quantidade de Inscricoes")
    ax2.set_title("Por Tipo de Garantia\nNormal vs RJ/Falencia", fontweight="bold")
    ax2.legend()
    ax2.grid(axis="y", alpha=0.3, linestyle="--")

    plt.suptitle("Analise de Recuperacao Judicial e Falencia - Base Unificada PGFN",
                 fontsize=14, fontweight="bold", y=1.02)
    plt.tight_layout()
    salvar_fig(fig, "rj_falencia_comparativo.png")

    # Exportar resumo
    resumo = pd.DataFrame({
        "METRICA": ["Total Inscricoes", "Em Recuperacao Judicial", "Em Falencia",
                    "RJ ou Falencia (uniao)", "% RJ/Falencia", "Valor Total RJ/Falencia"],
        "VALOR": [total, n_rj, n_falencia, n_rj_ou_falencia,
                  f"{n_rj_ou_falencia/total*100:.2f}%", fmt_brl(df_rj["VALOR_CONSOLIDADO_NUM"].sum())]
    })
    salvar_csv(resumo, "rj_falencia_resumo.csv")
    salvar_csv(cross_pivot.reset_index(), "rj_falencia_x_garantia.csv")

    return df_rj, cross_pivot


# =============================================================================
# ANALISE 3: COMPARATIVO TIPOS DE GARANTIA NO TEMPO
# =============================================================================

def analise_garantias_tempo(df):
    """Evolucao temporal por tipo de garantia."""
    print("\n" + "-" * 80)
    print("  ANALISE 3: COMPARATIVO TIPOS DE GARANTIA NO TEMPO")
    print("-" * 80)

    # Por ano e garantia
    anual_gar = df.groupby(["ANO", "GARANTIA"]).agg(
        N_INSCRICOES=("NUMERO_INSCRICAO", "count"),
        VALOR_TOTAL=("VALOR_CONSOLIDADO_NUM", "sum"),
    ).reset_index()

    anual_gar = anual_gar[(anual_gar["ANO"] >= 1990) & (anual_gar["ANO"] <= 2026)]
    anual_gar["ANO"] = anual_gar["ANO"].astype(int)

    salvar_csv(anual_gar, "garantias_evolucao_anual.csv")

    # Pivot para visualizacao
    pivot_inscr = anual_gar.pivot(index="ANO", columns="GARANTIA", values="N_INSCRICOES").fillna(0)
    pivot_valor = anual_gar.pivot(index="ANO", columns="GARANTIA", values="VALOR_TOTAL").fillna(0)

    # Cores por garantia
    cores = {
        "SEGURO GARANTIA": "#27ae60",
        "PENHORA": "#e74c3c",
        "CARTA FIANCA": "#3498db",
        "DEPOSITO": "#f39c12",
        "SEM GARANTIA": "#95a5a6",
    }

    # Grafico 1: Evolucao de inscricoes por tipo
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12))

    for garantia in pivot_inscr.columns:
        cor = cores.get(garantia, "#bdc3c7")
        ax1.plot(pivot_inscr.index, pivot_inscr[garantia],
                 label=garantia, color=cor, linewidth=2, marker="o", markersize=4)

    ax1.set_xlabel("Ano")
    ax1.set_ylabel("Quantidade de Inscricoes")
    ax1.set_title("Evolucao de Inscricoes por Tipo de Garantia", fontweight="bold")
    ax1.legend(loc="upper left")
    ax1.grid(axis="both", alpha=0.3, linestyle="--")

    # Grafico 2: Stacked area (proporcao)
    pivot_pct = pivot_inscr.div(pivot_inscr.sum(axis=1), axis=0) * 100
    ax2.stackplot(pivot_pct.index, [pivot_pct[col] for col in pivot_pct.columns],
                  labels=pivot_pct.columns,
                  colors=[cores.get(c, "#bdc3c7") for c in pivot_pct.columns],
                  alpha=0.7)
    ax2.set_xlabel("Ano")
    ax2.set_ylabel("Percentual (%)")
    ax2.set_title("Composicao Percentual por Tipo de Garantia", fontweight="bold")
    ax2.legend(loc="upper left")
    ax2.set_ylim(0, 100)

    plt.suptitle("Comparativo de Garantias no Tempo - Base Unificada PGFN",
                 fontsize=14, fontweight="bold", y=1.02)
    plt.tight_layout()
    salvar_fig(fig, "garantias_evolucao.png")

    # Primeira aparicao de cada garantia
    print(f"\n  PRIMEIRA INSCRICAO POR TIPO DE GARANTIA:")
    for garantia in df["GARANTIA"].unique():
        subset = df[df["GARANTIA"] == garantia]
        primeira = subset["DATA_DT"].min()
        if pd.notna(primeira):
            print(f"    {garantia:<20} {primeira.strftime('%d/%m/%Y')}")

    # SG vs Penhora especificamente
    print(f"\n  SEGURO GARANTIA vs PENHORA - EVOLUCAO:")
    sg_anual = pivot_inscr["SEGURO GARANTIA"] if "SEGURO GARANTIA" in pivot_inscr.columns else pd.Series()
    penh_anual = pivot_inscr["PENHORA"] if "PENHORA" in pivot_inscr.columns else pd.Series()

    anos_sg = sg_anual[sg_anual > 0].index.tolist() if len(sg_anual) > 0 else []
    anos_penh = penh_anual[penh_anual > 0].index.tolist() if len(penh_anual) > 0 else []

    if anos_sg:
        print(f"    SG - Primeiro ano com volume: {min(anos_sg)}")
    if anos_penh:
        print(f"    Penhora - Primeiro ano com volume: {min(anos_penh)}")

    # Cruzamento detalhado
    print(f"\n  VOLUMES NOS ULTIMOS 10 ANOS:")
    ultimos_10 = pivot_inscr.tail(10)
    print(f"\n  ANO   {'SG':>10} {'PENHORA':>10} {'CARTA F':>10} {'DEPOSITO':>10} {'SEM GAR':>10}")
    print("  " + "-" * 65)
    for ano in ultimos_10.index:
        sg = ultimos_10.loc[ano, "SEGURO GARANTIA"] if "SEGURO GARANTIA" in ultimos_10.columns else 0
        pe = ultimos_10.loc[ano, "PENHORA"] if "PENHORA" in ultimos_10.columns else 0
        cf = ultimos_10.loc[ano, "CARTA FIANCA"] if "CARTA FIANCA" in ultimos_10.columns else 0
        dp = ultimos_10.loc[ano, "DEPOSITO"] if "DEPOSITO" in ultimos_10.columns else 0
        sm = ultimos_10.loc[ano, "SEM GARANTIA"] if "SEM GARANTIA" in ultimos_10.columns else 0
        print(f"  {ano}   {int(sg):>10,} {int(pe):>10,} {int(cf):>10,} {int(dp):>10,} {int(sm):>10,}")

    return anual_gar, pivot_inscr


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 80)
    print("  ANALISE CRUZADA - BASE UNIFICADA PGFN")
    print("=" * 80)
    print(f"  Diretorio dados: {config.DATA_DIR}")

    setup()

    # Carregar e preparar dados
    df = carregar_base_unificada()
    df = preparar_dados(df)

    # Resumo inicial
    print(f"\n  RESUMO DA BASE:")
    print(f"    Inscricoes: {len(df):,}")
    print(f"    CNPJs unicos: {df['CPF_CNPJ'].nunique():,}")
    print(f"    Valor total: {fmt_brl(df['VALOR_CONSOLIDADO_NUM'].sum())}")
    print(f"    Periodo: {df['DATA_DT'].min().strftime('%d/%m/%Y')} a {df['DATA_DT'].max().strftime('%d/%m/%Y')}")

    # Executar analises
    analise_temporal_anual(df)
    analise_temporal_mensal_60m(df)
    analise_temporal_diaria(df)
    df_rj, cross_rj = analise_rj_falencia(df)
    anual_gar, pivot_gar = analise_garantias_tempo(df)

    # Lista de arquivos gerados
    print("\n" + "=" * 80)
    print("  ARQUIVOS GERADOS")
    print("=" * 80)

    arquivos = list(OUTPUT_DIR.glob("*"))
    csvs = sorted([f for f in arquivos if f.suffix == ".csv"])
    pngs = sorted([f for f in arquivos if f.suffix == ".png"])

    print(f"\n  CSVs ({len(csvs)}):")
    for f in csvs:
        print(f"    - {f.name}")

    print(f"\n  Graficos ({len(pngs)}):")
    for f in pngs:
        print(f"    - {f.name}")

    print("\n" + "=" * 80)
    print("  ANALISE CRUZADA CONCLUIDA!")
    print("=" * 80)


if __name__ == "__main__":
    main()
