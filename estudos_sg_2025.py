"""
ESTUDOS SEGURO GARANTIA 2025 - PGFN
====================================
Analise completa das inscricoes com Seguro Garantia no ano de 2025.

Le as 6 bases brutas PGFN, filtra SG + 2025, e gera:
- CSV consolidado com todas as colunas originais
- Series temporais (mensal e diaria)
- Distribuicao de ticket (percentis, histograma)
- Cruzamentos por tributo, regiao, UF
- Analise de recorrencia por empresa

Uso: python estudos_sg_2025.py
"""

import os
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from datetime import datetime

# Configuracao de paths
PGFN_DATA_DIR = Path(os.environ.get("PGFN_DATA_DIR", r"C:\Rodrigo\BasePGFN\2025"))
OUTPUT_DIR = PGFN_DATA_DIR / "outputs_sg_2025"

# Arquivos de entrada
BASES = [
    ("arquivo_lai_SIDA_1_202512.csv", "1a Regiao (DF/GO/MT/TO/Norte)"),
    ("arquivo_lai_SIDA_2_202512.csv", "2a Regiao (RJ/ES)"),
    ("arquivo_lai_SIDA_3_202512.csv", "3a Regiao (SP/MS)"),
    ("arquivo_lai_SIDA_4_202512.csv", "4a Regiao (RS/SC/PR)"),
    ("arquivo_lai_SIDA_5_202512.csv", "5a Regiao (PE/AL/PB/RN/CE/SE)"),
    ("arquivo_lai_SIDA_6_202512.csv", "6a Regiao (MG)"),
]


# =============================================================================
# FUNCOES AUXILIARES
# =============================================================================

def setup_output_dir():
    """Cria diretorio de saida se nao existir."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"  Diretorio de saida: {OUTPUT_DIR}")


def normalizar_colunas(df):
    """Normaliza nomes de colunas (strip, sem espacos extras)."""
    df.columns = df.columns.str.strip()
    return df


def converter_valor_brl(series):
    """Converte valor no formato brasileiro (1.234.567,89) para float."""
    return pd.to_numeric(
        series.astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False),
        errors="coerce"
    )


def extrair_cnpj_raiz(cpf_cnpj, tipo_pessoa):
    """Extrai os primeiros 8 digitos do CNPJ (raiz) para PJ."""
    doc = str(cpf_cnpj).strip()
    # Remover mascara
    doc_limpo = "".join(c for c in doc if c.isdigit())

    # Se for PJ (14 digitos), pegar raiz (8 primeiros)
    if len(doc_limpo) == 14:
        return doc_limpo[:8]
    return doc_limpo


def fmt_brl(valor):
    """Formata valor numerico no padrao brasileiro."""
    if pd.isna(valor):
        return "R$ -"
    if abs(valor) >= 1e12:
        return f"R$ {valor/1e12:.2f} tri"
    if abs(valor) >= 1e9:
        return f"R$ {valor/1e9:.2f} bi"
    if abs(valor) >= 1e6:
        return f"R$ {valor/1e6:.2f} mi"
    if abs(valor) >= 1e3:
        return f"R$ {valor/1e3:.1f} mil"
    return f"R$ {valor:,.2f}"


def salvar_csv(df, nome_arquivo):
    """Salva DataFrame como CSV no diretorio de saida."""
    path = OUTPUT_DIR / nome_arquivo
    df.to_csv(path, sep=";", index=False, encoding="utf-8-sig")
    print(f"    CSV salvo: {path.name}")
    return path


def salvar_grafico(fig, nome_arquivo, dpi=150):
    """Salva figura matplotlib no diretorio de saida."""
    path = OUTPUT_DIR / nome_arquivo
    fig.savefig(path, dpi=dpi, bbox_inches="tight")
    print(f"    Grafico salvo: {path.name}")
    return path


# =============================================================================
# LEITURA E FILTRAGEM
# =============================================================================

def ler_e_filtrar_base(arquivo, regiao_nome):
    """Le uma base e filtra SG + 2025."""
    filepath = PGFN_DATA_DIR / arquivo

    if not filepath.exists():
        print(f"    AVISO: Arquivo nao encontrado: {filepath}")
        return None

    print(f"  Lendo {arquivo}...")

    # Ler com chunks para melhor performance em arquivos grandes
    chunks = []
    total_bruto = 0

    try:
        for chunk in pd.read_csv(
            filepath,
            sep=";",
            encoding="latin1",
            low_memory=False,
            chunksize=100_000
        ):
            chunk = normalizar_colunas(chunk)
            total_bruto += len(chunk)

            # Filtrar SEGURO GARANTIA (robusto: upper + contains)
            mask_sg = chunk["SITUACAO_INSCRICAO"].astype(str).str.upper().str.strip().str.contains(
                "SEGURO GARANTIA", na=False
            )
            chunk_sg = chunk[mask_sg].copy()

            if len(chunk_sg) > 0:
                chunks.append(chunk_sg)

        if not chunks:
            print(f"    Nenhum SG encontrado em {arquivo}")
            return None

        df = pd.concat(chunks, ignore_index=True)

    except Exception as e:
        print(f"    ERRO ao ler {arquivo}: {e}")
        return None

    # Converter DATA_INSCRICAO para datetime
    df["DATA_INSCRICAO_DT"] = pd.to_datetime(
        df["DATA_INSCRICAO"],
        errors="coerce",
        dayfirst=True
    )

    # Filtrar apenas 2025
    mask_2025 = (
        (df["DATA_INSCRICAO_DT"] >= "2025-01-01") &
        (df["DATA_INSCRICAO_DT"] <= "2025-12-31")
    )
    df_2025 = df[mask_2025].copy()

    # Adicionar coluna de regiao
    df_2025["REGIAO"] = regiao_nome

    print(f"    Bruto: {total_bruto:,} | SG total: {len(df):,} | SG 2025: {len(df_2025):,}")

    return df_2025


def carregar_todas_bases():
    """Carrega e consolida todas as 6 bases."""
    print("\n" + "=" * 80)
    print("  LEITURA DAS 6 BASES PGFN")
    print("=" * 80)

    partes = []
    for arquivo, regiao in BASES:
        df = ler_e_filtrar_base(arquivo, regiao)
        if df is not None and len(df) > 0:
            partes.append(df)

    if not partes:
        print("\nERRO: Nenhum registro de SG 2025 encontrado nas bases.")
        sys.exit(1)

    df_consolidado = pd.concat(partes, ignore_index=True)
    print(f"\n  TOTAL CONSOLIDADO: {len(df_consolidado):,} inscricoes SG 2025")

    return df_consolidado


def criar_colunas_derivadas(df):
    """Cria colunas derivadas sem remover originais."""
    df = df.copy()

    # VALOR_CONSOLIDADO numerico
    df["VALOR_CONSOLIDADO"] = converter_valor_brl(df["VALOR_CONSOLIDADO"])

    # ANO e MES
    df["ANO"] = df["DATA_INSCRICAO_DT"].dt.year
    df["MES"] = df["DATA_INSCRICAO_DT"].dt.to_period("M").astype(str)
    df["DATA"] = df["DATA_INSCRICAO_DT"].dt.date

    # FLAG_PJ
    df["FLAG_PJ"] = df["TIPO_PESSOA"].astype(str).str.upper().str.contains("JURID", na=False)

    # CNPJ_RAIZ (apenas para PJ)
    df["CNPJ_RAIZ"] = df.apply(
        lambda row: extrair_cnpj_raiz(row["CPF_CNPJ"], row["TIPO_PESSOA"]) if row["FLAG_PJ"] else None,
        axis=1
    )

    # TRIBUTO normalizado
    df["TRIBUTO"] = df["RECEITA_PRINCIPAL"].astype(str).str.strip().str.upper()

    return df


# =============================================================================
# ANALISE A - SERIE MENSAL 2025
# =============================================================================

def analise_serie_mensal(df):
    """Gera serie mensal com estatisticas e grafico."""
    print("\n" + "-" * 80)
    print("  ANALISE A: SERIE MENSAL 2025")
    print("-" * 80)

    mensal = df.groupby("MES").agg(
        N_INSCRICOES=("NUMERO_INSCRICAO", "count"),
        VALOR_TOTAL=("VALOR_CONSOLIDADO", "sum"),
        TICKET_MEDIO=("VALOR_CONSOLIDADO", "mean"),
        TICKET_MEDIANO=("VALOR_CONSOLIDADO", "median"),
        CNPJS_DISTINTOS=("CPF_CNPJ", "nunique"),
    ).reset_index().sort_values("MES")

    # Media movel 3 meses
    mensal["MM3_INSCRICOES"] = mensal["N_INSCRICOES"].rolling(3, min_periods=1).mean()
    mensal["MM3_VALOR"] = mensal["VALOR_TOTAL"].rolling(3, min_periods=1).mean()

    # Salvar CSV
    salvar_csv(mensal, "sg_2025_serie_mensal.csv")

    # Grafico com eixo secundario
    fig, ax1 = plt.subplots(figsize=(14, 7))

    x = range(len(mensal))

    # Barras: contagem
    bars = ax1.bar(x, mensal["N_INSCRICOES"], color="#3498db", alpha=0.7, label="Inscricoes")
    ax1.set_xlabel("Mes", fontsize=11)
    ax1.set_ylabel("Quantidade de Inscricoes", color="#3498db", fontsize=11)
    ax1.tick_params(axis="y", labelcolor="#3498db")
    ax1.set_xticks(x)
    ax1.set_xticklabels(mensal["MES"], rotation=45, ha="right")

    # Linha MM3 inscricoes
    ax1.plot(x, mensal["MM3_INSCRICOES"], color="#2980b9", linewidth=2,
             linestyle="--", label="MM3 Inscricoes")

    # Eixo secundario: valor total
    ax2 = ax1.twinx()
    ax2.plot(x, mensal["VALOR_TOTAL"] / 1e9, color="#e74c3c", linewidth=2.5,
             marker="o", markersize=6, label="Valor Total (R$ bi)")
    ax2.set_ylabel("Valor Total (R$ bilhoes)", color="#e74c3c", fontsize=11)
    ax2.tick_params(axis="y", labelcolor="#e74c3c")

    # Titulo e legenda
    ax1.set_title("Serie Mensal - Seguro Garantia 2025\nInscricoes (barras) vs Valor Total (linha)",
                  fontsize=13, fontweight="bold")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    ax1.grid(axis="y", alpha=0.3, linestyle="--")
    plt.tight_layout()
    salvar_grafico(fig, "sg_2025_serie_mensal.png")
    plt.close(fig)

    # Print resumo
    print(f"\n  MES          INSCR    VALOR TOTAL     TICKET MED    TICKET MEDIANO")
    print("  " + "-" * 70)
    for _, row in mensal.iterrows():
        print(f"  {row['MES']}    {row['N_INSCRICOES']:>6,}    {fmt_brl(row['VALOR_TOTAL']):>14}    "
              f"{fmt_brl(row['TICKET_MEDIO']):>12}    {fmt_brl(row['TICKET_MEDIANO']):>14}")

    return mensal


# =============================================================================
# ANALISE B - SERIE DIARIA 2025
# =============================================================================

def analise_serie_diaria(df):
    """Gera serie diaria com estatisticas e grafico."""
    print("\n" + "-" * 80)
    print("  ANALISE B: SERIE DIARIA 2025")
    print("-" * 80)

    diaria = df.groupby("DATA").agg(
        N_INSCRICOES=("NUMERO_INSCRICAO", "count"),
        VALOR_TOTAL=("VALOR_CONSOLIDADO", "sum"),
    ).reset_index().sort_values("DATA")

    diaria["DATA"] = pd.to_datetime(diaria["DATA"])

    # Salvar CSV
    salvar_csv(diaria, "sg_2025_serie_diaria.csv")

    # Grafico scatter
    fig, ax1 = plt.subplots(figsize=(16, 7))

    # Scatter: contagem (tamanho proporcional)
    sizes = diaria["N_INSCRICOES"] / diaria["N_INSCRICOES"].max() * 200 + 10
    scatter = ax1.scatter(diaria["DATA"], diaria["N_INSCRICOES"],
                          s=sizes, c="#3498db", alpha=0.5, edgecolors="white", linewidths=0.5)
    ax1.set_xlabel("Data", fontsize=11)
    ax1.set_ylabel("Quantidade de Inscricoes", color="#3498db", fontsize=11)
    ax1.tick_params(axis="y", labelcolor="#3498db")

    # Eixo secundario: valor total
    ax2 = ax1.twinx()
    ax2.plot(diaria["DATA"], diaria["VALOR_TOTAL"] / 1e6, color="#e74c3c",
             alpha=0.7, linewidth=1, label="Valor Total (R$ mi)")
    ax2.fill_between(diaria["DATA"], 0, diaria["VALOR_TOTAL"] / 1e6,
                     color="#e74c3c", alpha=0.1)
    ax2.set_ylabel("Valor Total (R$ milhoes)", color="#e74c3c", fontsize=11)
    ax2.tick_params(axis="y", labelcolor="#e74c3c")

    ax1.set_title("Serie Diaria - Seguro Garantia 2025\nDispersao de Inscricoes (scatter) + Valor Total (area)",
                  fontsize=13, fontweight="bold")

    ax1.grid(axis="both", alpha=0.3, linestyle="--")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    salvar_grafico(fig, "sg_2025_serie_diaria.png")
    plt.close(fig)

    # Estatisticas
    print(f"\n  Dias com inscricoes: {len(diaria):,}")
    print(f"  Media diaria: {diaria['N_INSCRICOES'].mean():.1f} inscricoes")
    print(f"  Max diario: {diaria['N_INSCRICOES'].max():,} inscricoes")
    print(f"  Data com mais inscricoes: {diaria.loc[diaria['N_INSCRICOES'].idxmax(), 'DATA'].strftime('%d/%m/%Y')}")

    return diaria


# =============================================================================
# ANALISE C - DISTRIBUICAO DE TICKET
# =============================================================================

def analise_distribuicao_ticket(df):
    """Gera histograma e percentis de ticket."""
    print("\n" + "-" * 80)
    print("  ANALISE C: DISTRIBUICAO DE TICKET 2025")
    print("-" * 80)

    valores = df["VALOR_CONSOLIDADO"].dropna()
    valores_positivos = valores[valores > 0]

    # Percentis
    percentis = {
        "P50": valores.quantile(0.50),
        "P75": valores.quantile(0.75),
        "P90": valores.quantile(0.90),
        "P95": valores.quantile(0.95),
        "P99": valores.quantile(0.99),
        "MIN": valores.min(),
        "MAX": valores.max(),
        "MEDIA": valores.mean(),
        "DESVIO": valores.std(),
    }

    df_percentis = pd.DataFrame([percentis])

    # Top 50 valores
    top50 = df.nlargest(50, "VALOR_CONSOLIDADO")[
        ["CPF_CNPJ", "NOME_DEVEDOR", "UF_DEVEDOR", "TRIBUTO", "VALOR_CONSOLIDADO", "DATA_INSCRICAO"]
    ].copy()

    # Salvar CSVs
    salvar_csv(df_percentis, "sg_2025_ticket_percentis.csv")
    salvar_csv(top50, "sg_2025_ticket_top50.csv")

    # Grafico: histograma em escala log
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    # Histograma log-scale
    bins_log = np.logspace(np.log10(valores_positivos.min()),
                           np.log10(valores_positivos.max()), 50)
    ax1.hist(valores_positivos, bins=bins_log, color="#27ae60", alpha=0.7, edgecolor="white")
    ax1.set_xscale("log")
    ax1.set_xlabel("Valor (R$) - Escala Log", fontsize=11)
    ax1.set_ylabel("Frequencia", fontsize=11)
    ax1.set_title("Distribuicao de Ticket (escala log)", fontweight="bold")

    # Linhas de percentis
    for p, val in [("P50", percentis["P50"]), ("P90", percentis["P90"]), ("P99", percentis["P99"])]:
        ax1.axvline(val, color="#e74c3c", linestyle="--", alpha=0.7)
        ax1.text(val, ax1.get_ylim()[1] * 0.9, f" {p}\n {fmt_brl(val)}", fontsize=8)

    ax1.grid(axis="y", alpha=0.3, linestyle="--")

    # Histograma de log10(valor)
    log_valores = np.log10(valores_positivos)
    ax2.hist(log_valores, bins=50, color="#9b59b6", alpha=0.7, edgecolor="white")
    ax2.set_xlabel("log10(Valor)", fontsize=11)
    ax2.set_ylabel("Frequencia", fontsize=11)
    ax2.set_title("Distribuicao de log10(Valor)", fontweight="bold")
    ax2.grid(axis="y", alpha=0.3, linestyle="--")

    plt.suptitle("Analise de Ticket - Seguro Garantia 2025", fontsize=13, fontweight="bold", y=1.02)
    plt.tight_layout()
    salvar_grafico(fig, "sg_2025_ticket_hist.png")
    plt.close(fig)

    # Print resumo
    print(f"\n  PERCENTIS DE TICKET:")
    for nome, val in percentis.items():
        print(f"    {nome}: {fmt_brl(val)}")

    return df_percentis, top50


# =============================================================================
# ANALISE D - CRUZAMENTO POR TRIBUTO
# =============================================================================

def analise_por_tributo(df):
    """Gera analise cruzada por tributo."""
    print("\n" + "-" * 80)
    print("  ANALISE D: CRUZAMENTO POR TRIBUTO 2025")
    print("-" * 80)

    tributo = df.groupby("TRIBUTO").agg(
        N_INSCRICOES=("NUMERO_INSCRICAO", "count"),
        CNPJS_DISTINTOS=("CPF_CNPJ", "nunique"),
        VALOR_TOTAL=("VALOR_CONSOLIDADO", "sum"),
        TICKET_MEDIANO=("VALOR_CONSOLIDADO", "median"),
        TICKET_P90=("VALOR_CONSOLIDADO", lambda x: x.quantile(0.90)),
    ).reset_index()

    tributo_valor = tributo.sort_values("VALOR_TOTAL", ascending=False)
    tributo_ticket = tributo.sort_values("TICKET_MEDIANO", ascending=False)

    # Salvar CSV
    salvar_csv(tributo_valor, "sg_2025_por_tributo.csv")

    # Grafico: top 15 por valor total
    top15 = tributo_valor.head(15).copy()

    fig, ax1 = plt.subplots(figsize=(14, 8))

    y = range(len(top15))
    bars = ax1.barh(y, top15["VALOR_TOTAL"] / 1e9, color="#3498db", alpha=0.7)
    ax1.set_yticks(y)
    ax1.set_yticklabels([t[:40] for t in top15["TRIBUTO"]], fontsize=9)
    ax1.set_xlabel("Valor Total (R$ bilhoes)", color="#3498db", fontsize=11)
    ax1.tick_params(axis="x", labelcolor="#3498db")
    ax1.invert_yaxis()

    # Eixo secundario: ticket mediano
    ax2 = ax1.twiny()
    ax2.plot(top15["TICKET_MEDIANO"] / 1e6, y, color="#e74c3c",
             marker="o", markersize=8, linewidth=2, label="Ticket Mediano")
    ax2.set_xlabel("Ticket Mediano (R$ milhoes)", color="#e74c3c", fontsize=11)
    ax2.tick_params(axis="x", labelcolor="#e74c3c")

    ax1.set_title("Top 15 Tributos por Valor Total - Seguro Garantia 2025\n"
                  "Barras = Valor Total | Pontos = Ticket Mediano",
                  fontsize=13, fontweight="bold")

    ax1.grid(axis="x", alpha=0.3, linestyle="--")
    plt.tight_layout()
    salvar_grafico(fig, "sg_2025_por_tributo_top15.png")
    plt.close(fig)

    # Print resumo
    print(f"\n  TOP 10 TRIBUTOS POR VALOR TOTAL:")
    print(f"  {'TRIBUTO':<45} {'INSCR':>8} {'VALOR TOTAL':>16} {'TICKET MED':>14}")
    print("  " + "-" * 85)
    for _, row in tributo_valor.head(10).iterrows():
        print(f"  {row['TRIBUTO'][:43]:<45} {row['N_INSCRICOES']:>8,} "
              f"{fmt_brl(row['VALOR_TOTAL']):>16} {fmt_brl(row['TICKET_MEDIANO']):>14}")

    return tributo


# =============================================================================
# ANALISE E - CRUZAMENTO POR REGIAO E UF
# =============================================================================

def analise_por_regiao_uf(df):
    """Gera analise cruzada por regiao e UF."""
    print("\n" + "-" * 80)
    print("  ANALISE E: CRUZAMENTO POR REGIAO E UF 2025")
    print("-" * 80)

    # Por regiao
    regiao = df.groupby("REGIAO").agg(
        N_INSCRICOES=("NUMERO_INSCRICAO", "count"),
        CNPJS_DISTINTOS=("CPF_CNPJ", "nunique"),
        VALOR_TOTAL=("VALOR_CONSOLIDADO", "sum"),
        TICKET_MEDIANO=("VALOR_CONSOLIDADO", "median"),
    ).reset_index().sort_values("VALOR_TOTAL", ascending=False)

    # Por UF
    uf = df.groupby("UF_DEVEDOR").agg(
        N_INSCRICOES=("NUMERO_INSCRICAO", "count"),
        CNPJS_DISTINTOS=("CPF_CNPJ", "nunique"),
        VALOR_TOTAL=("VALOR_CONSOLIDADO", "sum"),
        TICKET_MEDIANO=("VALOR_CONSOLIDADO", "median"),
    ).reset_index().sort_values("VALOR_TOTAL", ascending=False)

    # Salvar CSVs
    salvar_csv(regiao, "sg_2025_por_regiao.csv")
    salvar_csv(uf, "sg_2025_por_uf.csv")

    # Grafico: top 15 UFs
    top15_uf = uf.head(15).copy()

    fig, ax1 = plt.subplots(figsize=(14, 8))

    x = range(len(top15_uf))
    bars = ax1.bar(x, top15_uf["VALOR_TOTAL"] / 1e9, color="#27ae60", alpha=0.7)
    ax1.set_xticks(x)
    ax1.set_xticklabels(top15_uf["UF_DEVEDOR"], rotation=45, ha="right")
    ax1.set_xlabel("UF", fontsize=11)
    ax1.set_ylabel("Valor Total (R$ bilhoes)", color="#27ae60", fontsize=11)
    ax1.tick_params(axis="y", labelcolor="#27ae60")

    # Eixo secundario: contagem
    ax2 = ax1.twinx()
    ax2.plot(x, top15_uf["N_INSCRICOES"], color="#e74c3c",
             marker="s", markersize=8, linewidth=2, label="N Inscricoes")
    ax2.set_ylabel("Quantidade de Inscricoes", color="#e74c3c", fontsize=11)
    ax2.tick_params(axis="y", labelcolor="#e74c3c")

    ax1.set_title("Top 15 UFs por Valor Total - Seguro Garantia 2025\n"
                  "Barras = Valor Total | Linha = Contagem",
                  fontsize=13, fontweight="bold")

    ax1.grid(axis="y", alpha=0.3, linestyle="--")
    plt.tight_layout()
    salvar_grafico(fig, "sg_2025_por_uf_top15.png")
    plt.close(fig)

    # Print resumo
    print(f"\n  POR REGIAO:")
    for _, row in regiao.iterrows():
        print(f"    {row['REGIAO']}: {row['N_INSCRICOES']:,} inscr | {fmt_brl(row['VALOR_TOTAL'])}")

    print(f"\n  TOP 10 UFs:")
    for _, row in uf.head(10).iterrows():
        print(f"    {row['UF_DEVEDOR']}: {row['N_INSCRICOES']:,} inscr | {fmt_brl(row['VALOR_TOTAL'])}")

    return regiao, uf


# =============================================================================
# ANALISE F - RECORRENCIA POR EMPRESA
# =============================================================================

def analise_recorrencia(df):
    """Gera analise de recorrencia por CNPJ_RAIZ."""
    print("\n" + "-" * 80)
    print("  ANALISE F: RECORRENCIA POR EMPRESA 2025")
    print("-" * 80)

    # Apenas PJ com CNPJ_RAIZ valido
    df_pj = df[df["FLAG_PJ"] & df["CNPJ_RAIZ"].notna()].copy()

    # Agregar por CNPJ_RAIZ
    por_grupo = df_pj.groupby("CNPJ_RAIZ").agg(
        N_INSCRICOES=("NUMERO_INSCRICAO", "count"),
        VALOR_TOTAL=("VALOR_CONSOLIDADO", "sum"),
        TICKET_MEDIANO=("VALOR_CONSOLIDADO", "median"),
        NOME=("NOME_DEVEDOR", "first"),
        UF=("UF_DEVEDOR", "first"),
    ).reset_index()

    # Faixas de recorrencia
    faixas = [
        ("1", 1, 1),
        ("2-3", 2, 3),
        ("4-5", 4, 5),
        ("6-10", 6, 10),
        ("11-20", 11, 20),
        ("21+", 21, 999999),
    ]

    resultados_faixas = []
    for nome_faixa, vmin, vmax in faixas:
        subset = por_grupo[(por_grupo["N_INSCRICOES"] >= vmin) & (por_grupo["N_INSCRICOES"] <= vmax)]
        if len(subset) == 0:
            continue
        resultados_faixas.append({
            "FAIXA": nome_faixa,
            "QTD_EMPRESAS": len(subset),
            "PCT_EMPRESAS": len(subset) / len(por_grupo) * 100,
            "VALOR_TOTAL": subset["VALOR_TOTAL"].sum(),
            "TICKET_MEDIANO_EMPRESA": subset["VALOR_TOTAL"].median(),
        })

    df_faixas = pd.DataFrame(resultados_faixas)

    # Top 50 grupos por valor
    top50_grupos = por_grupo.nlargest(50, "VALOR_TOTAL")[
        ["CNPJ_RAIZ", "NOME", "UF", "N_INSCRICOES", "VALOR_TOTAL", "TICKET_MEDIANO"]
    ]

    # Salvar CSVs
    salvar_csv(df_faixas, "sg_2025_recorrencia_faixas.csv")
    salvar_csv(top50_grupos, "sg_2025_top50_grupos.csv")

    # Grafico
    fig, ax1 = plt.subplots(figsize=(12, 7))

    x = range(len(df_faixas))
    bars = ax1.bar(x, df_faixas["QTD_EMPRESAS"], color="#9b59b6", alpha=0.7)
    ax1.set_xticks(x)
    ax1.set_xticklabels(df_faixas["FAIXA"])
    ax1.set_xlabel("Faixa de Inscricoes por Empresa", fontsize=11)
    ax1.set_ylabel("Quantidade de Empresas", color="#9b59b6", fontsize=11)
    ax1.tick_params(axis="y", labelcolor="#9b59b6")

    # Eixo secundario: valor total
    ax2 = ax1.twinx()
    ax2.plot(x, df_faixas["VALOR_TOTAL"] / 1e9, color="#e74c3c",
             marker="o", markersize=10, linewidth=2.5, label="Valor Total (R$ bi)")
    ax2.set_ylabel("Valor Total (R$ bilhoes)", color="#e74c3c", fontsize=11)
    ax2.tick_params(axis="y", labelcolor="#e74c3c")

    ax1.set_title("Recorrencia por Empresa - Seguro Garantia 2025\n"
                  "Barras = Qtd Empresas | Linha = Valor Total",
                  fontsize=13, fontweight="bold")

    # Adicionar % nas barras
    for i, (bar, pct) in enumerate(zip(bars, df_faixas["PCT_EMPRESAS"])):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                 f"{pct:.1f}%", ha="center", fontsize=9, fontweight="bold")

    ax1.grid(axis="y", alpha=0.3, linestyle="--")
    plt.tight_layout()
    salvar_grafico(fig, "sg_2025_recorrencia.png")
    plt.close(fig)

    # Print resumo
    print(f"\n  FAIXAS DE RECORRENCIA:")
    print(f"  {'FAIXA':<10} {'EMPRESAS':>10} {'%':>8} {'VALOR TOTAL':>16} {'TICKET MED':>14}")
    print("  " + "-" * 60)
    for _, row in df_faixas.iterrows():
        print(f"  {row['FAIXA']:<10} {row['QTD_EMPRESAS']:>10,} {row['PCT_EMPRESAS']:>7.1f}% "
              f"{fmt_brl(row['VALOR_TOTAL']):>16} {fmt_brl(row['TICKET_MEDIANO_EMPRESA']):>14}")

    print(f"\n  TOP 5 GRUPOS (CNPJ_RAIZ) POR VALOR:")
    for i, (_, row) in enumerate(top50_grupos.head(5).iterrows(), 1):
        print(f"    {i}. {row['CNPJ_RAIZ']} | {row['N_INSCRICOES']} inscr | {fmt_brl(row['VALOR_TOTAL'])}")

    return df_faixas, top50_grupos


# =============================================================================
# ANALISE G - SANITY CHECKS
# =============================================================================

def sanity_checks(df, df_original_count):
    """Executa validacoes e imprime resumo."""
    print("\n" + "=" * 80)
    print("  ANALISE G: SANITY CHECKS E QUALIDADE")
    print("=" * 80)

    # Validar periodo
    data_min = df["DATA_INSCRICAO_DT"].min()
    data_max = df["DATA_INSCRICAO_DT"].max()

    print(f"\n  VALIDACOES:")
    print(f"    Periodo: {data_min.strftime('%d/%m/%Y')} a {data_max.strftime('%d/%m/%Y')}")

    if data_min.year != 2025 or data_max.year != 2025:
        print(f"    AVISO: Datas fora de 2025 detectadas!")
    else:
        print(f"    OK: Todas as datas estao em 2025")

    # Nulls criticos
    nulls = {
        "CPF_CNPJ": df["CPF_CNPJ"].isna().sum(),
        "VALOR_CONSOLIDADO": df["VALOR_CONSOLIDADO"].isna().sum(),
        "DATA_INSCRICAO_DT": df["DATA_INSCRICAO_DT"].isna().sum(),
        "SITUACAO_INSCRICAO": df["SITUACAO_INSCRICAO"].isna().sum(),
    }

    print(f"\n  VALORES NULOS:")
    for col, count in nulls.items():
        pct = count / len(df) * 100
        status = "OK" if count == 0 else f"ATENCAO ({pct:.2f}%)"
        print(f"    {col}: {count:,} - {status}")

    # Resumo geral
    print(f"\n  RESUMO GERAL:")
    print(f"    Total registros SG 2025: {len(df):,}")
    print(f"    CNPJs unicos: {df['CPF_CNPJ'].nunique():,}")
    print(f"    CNPJ_RAIZ unicos (PJ): {df[df['FLAG_PJ']]['CNPJ_RAIZ'].nunique():,}")
    print(f"    Valor total: {fmt_brl(df['VALOR_CONSOLIDADO'].sum())}")
    print(f"    Ticket medio: {fmt_brl(df['VALOR_CONSOLIDADO'].mean())}")
    print(f"    Ticket mediano: {fmt_brl(df['VALOR_CONSOLIDADO'].median())}")

    # Por tipo pessoa
    print(f"\n  POR TIPO PESSOA:")
    for tp in df["TIPO_PESSOA"].unique():
        subset = df[df["TIPO_PESSOA"] == tp]
        print(f"    {tp}: {len(subset):,} ({len(subset)/len(df)*100:.1f}%)")


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 80)
    print("  ESTUDOS SEGURO GARANTIA 2025 - PGFN")
    print("=" * 80)
    print(f"  Data: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Diretorio dados: {PGFN_DATA_DIR}")

    # Setup
    setup_output_dir()

    # Carregar todas as bases
    df = carregar_todas_bases()
    total_original = len(df)

    # Criar colunas derivadas
    print("\n  Criando colunas derivadas...")
    df = criar_colunas_derivadas(df)

    # Exportar CSV consolidado com TODAS as colunas
    print("\n" + "-" * 80)
    print("  EXPORTANDO CSV CONSOLIDADO")
    print("-" * 80)

    # Garantir que VALOR_CONSOLIDADO esteja numerico no CSV (ponto decimal)
    df_export = df.copy()
    df_export["VALOR_CONSOLIDADO"] = df_export["VALOR_CONSOLIDADO"].apply(
        lambda x: f"{x:.2f}" if pd.notna(x) else ""
    )
    salvar_csv(df_export, "sg_2025_todas_colunas.csv")
    print(f"    Colunas: {len(df.columns)}")
    print(f"    Linhas: {len(df):,}")

    # Executar analises
    analise_serie_mensal(df)
    analise_serie_diaria(df)
    analise_distribuicao_ticket(df)
    analise_por_tributo(df)
    analise_por_regiao_uf(df)
    analise_recorrencia(df)
    sanity_checks(df, total_original)

    # Lista de arquivos gerados
    print("\n" + "=" * 80)
    print("  ARQUIVOS GERADOS")
    print("=" * 80)

    arquivos = list(OUTPUT_DIR.glob("*"))
    csvs = [f for f in arquivos if f.suffix == ".csv"]
    pngs = [f for f in arquivos if f.suffix == ".png"]

    print(f"\n  CSVs ({len(csvs)}):")
    for f in sorted(csvs):
        print(f"    - {f.name}")

    print(f"\n  Graficos ({len(pngs)}):")
    for f in sorted(pngs):
        print(f"    - {f.name}")

    print("\n" + "=" * 80)
    print("  ESTUDOS SG 2025 CONCLUIDOS!")
    print("=" * 80)


if __name__ == "__main__":
    main()
