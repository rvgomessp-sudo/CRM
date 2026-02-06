"""
Analise temporal de inscricoes com Seguro Garantia - PGFN

Gera duas visualizacoes:
1) Dispersao diaria: scatter plot mostrando cada inscricao no tempo
2) Serie mensal: histograma agregado por mes

Entrada: pgfn_seguro_garantia_todas_inscricoes.csv
Saidas:
- dispersao_diaria_seguro_garantia.png
- serie_mensal_seguro_garantia.png
"""

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import sys

import config
from utils import salvar_grafico


def carregar_dados():
    """Carrega o CSV consolidado de Seguro Garantia."""
    arquivo = config.get_data_path("pgfn_seguro_garantia_todas_inscricoes.csv")

    print(f"  Lendo: {arquivo}")

    if not arquivo.exists():
        print(f"  ERRO: Arquivo nao encontrado: {arquivo}")
        print(f"  Execute primeiro: python extrair_seguro_garantia_pgfn.py")
        sys.exit(1)

    df = pd.read_csv(arquivo, sep=";", encoding="utf-8-sig", low_memory=False)
    print(f"  Registros carregados: {len(df):,}")

    return df


def preparar_dados(df):
    """Converte DATA_INSCRICAO para datetime e adiciona colunas auxiliares."""
    df = df.copy()

    # Converter DATA_INSCRICAO para datetime
    df["DATA_INSCRICAO"] = pd.to_datetime(
        df["DATA_INSCRICAO"],
        errors="coerce",
        dayfirst=True
    )

    # Remover registros sem data valida
    df_valido = df[df["DATA_INSCRICAO"].notna()].copy()
    removidos = len(df) - len(df_valido)
    if removidos > 0:
        print(f"  Registros sem data valida removidos: {removidos:,}")

    # Adicionar colunas auxiliares para agregacao
    df_valido["ANO"] = df_valido["DATA_INSCRICAO"].dt.year
    df_valido["MES"] = df_valido["DATA_INSCRICAO"].dt.month
    df_valido["ANO_MES"] = df_valido["DATA_INSCRICAO"].dt.to_period("M")

    print(f"  Registros com data valida: {len(df_valido):,}")
    print(f"  Periodo: {df_valido['DATA_INSCRICAO'].min().strftime('%d/%m/%Y')} a "
          f"{df_valido['DATA_INSCRICAO'].max().strftime('%d/%m/%Y')}")

    return df_valido


def gerar_dispersao_diaria(df):
    """Gera scatter plot com dispersao diaria das inscricoes."""
    print("\n  Gerando grafico de dispersao diaria...")

    fig, ax = plt.subplots(figsize=(16, 8))

    # Ordenar por data
    df_sorted = df.sort_values("DATA_INSCRICAO")
    datas = df_sorted["DATA_INSCRICAO"].values

    # Criar jitter no eixo Y para melhor visualizacao
    # (evita sobreposicao total de pontos no mesmo dia)
    np.random.seed(42)
    jitter = np.random.uniform(0, 1, len(datas))

    # Scatter plot
    ax.scatter(datas, jitter, alpha=0.3, s=5, c="#2980b9", edgecolors="none")

    # Formatacao do eixo X
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator(bymonth=[1, 4, 7, 10]))

    # Labels e titulo
    ax.set_xlabel("Data de Inscricao", fontsize=11)
    ax.set_ylabel("Dispersao (jitter)", fontsize=11)
    ax.set_title(f"Dispersao Diaria de Inscricoes com Seguro Garantia - PGFN\n"
                 f"Total: {len(df):,} inscricoes | "
                 f"Periodo: {df['DATA_INSCRICAO'].min().strftime('%Y')} a "
                 f"{df['DATA_INSCRICAO'].max().strftime('%Y')}",
                 fontsize=13, fontweight="bold")

    # Ocultar valores do eixo Y (jitter nao tem significado real)
    ax.set_yticks([])
    ax.set_yticklabels([])

    # Grid vertical para facilitar leitura temporal
    ax.grid(axis="x", alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    # Rotacionar labels do eixo X
    plt.xticks(rotation=45, ha="right")

    # Remover bordas
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)

    plt.tight_layout()
    return fig


def gerar_serie_mensal(df):
    """Gera histograma/serie temporal agregada por mes."""
    print("  Gerando serie mensal...")

    # Agregar por mes
    mensal = df.groupby("ANO_MES").agg(
        QTD_INSCRICOES=("NUMERO_INSCRICAO", "count"),
        VALOR_TOTAL=("VALOR_CONSOLIDADO", "sum") if "VALOR_CONSOLIDADO" in df.columns else ("NUMERO_INSCRICAO", "count"),
    ).reset_index()

    mensal = mensal.sort_values("ANO_MES")

    # Converter Period para datetime para plotagem
    mensal["DATA_MES"] = mensal["ANO_MES"].dt.to_timestamp()

    fig, ax = plt.subplots(figsize=(16, 8))

    # Barras
    bars = ax.bar(mensal["DATA_MES"], mensal["QTD_INSCRICOES"],
                  width=25, color="#27ae60", edgecolor="white", alpha=0.85)

    # Linha de tendencia (media movel 12 meses)
    if len(mensal) >= 12:
        mensal["MM12"] = mensal["QTD_INSCRICOES"].rolling(window=12, min_periods=1).mean()
        ax.plot(mensal["DATA_MES"], mensal["MM12"], color="#c0392b",
                linewidth=2.5, label="Media Movel 12 meses", linestyle="-")
        ax.legend(loc="upper left", fontsize=10)

    # Formatacao do eixo X
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

    # Labels e titulo
    ax.set_xlabel("Mes", fontsize=11)
    ax.set_ylabel("Quantidade de Inscricoes", fontsize=11)
    ax.set_title(f"Serie Mensal de Inscricoes com Seguro Garantia - PGFN\n"
                 f"Total: {mensal['QTD_INSCRICOES'].sum():,} inscricoes | "
                 f"{len(mensal)} meses",
                 fontsize=13, fontweight="bold")

    # Grid
    ax.grid(axis="y", alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    # Rotacionar labels
    plt.xticks(rotation=45, ha="right")

    # Remover bordas
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()

    return fig, mensal


def imprimir_estatisticas_mensais(mensal):
    """Imprime estatisticas da serie mensal."""
    print("\n" + "-" * 60)
    print("  ESTATISTICAS DA SERIE MENSAL")
    print("-" * 60)

    total = mensal["QTD_INSCRICOES"].sum()
    media = mensal["QTD_INSCRICOES"].mean()
    mediana = mensal["QTD_INSCRICOES"].median()
    maximo = mensal["QTD_INSCRICOES"].max()
    minimo = mensal["QTD_INSCRICOES"].min()
    desvio = mensal["QTD_INSCRICOES"].std()

    print(f"  Total de inscricoes: {total:,}")
    print(f"  Meses analisados: {len(mensal)}")
    print(f"  Media mensal: {media:,.1f}")
    print(f"  Mediana mensal: {mediana:,.1f}")
    print(f"  Maximo mensal: {maximo:,}")
    print(f"  Minimo mensal: {minimo:,}")
    print(f"  Desvio padrao: {desvio:,.1f}")

    # Top 5 meses
    print("\n  TOP 5 MESES COM MAIS INSCRICOES:")
    top5 = mensal.nlargest(5, "QTD_INSCRICOES")
    for i, (_, row) in enumerate(top5.iterrows(), 1):
        print(f"    {i}. {row['ANO_MES']}: {row['QTD_INSCRICOES']:,}")

    # Analise por ano
    print("\n  AGREGADO POR ANO:")
    mensal["ANO"] = mensal["ANO_MES"].dt.year
    por_ano = mensal.groupby("ANO")["QTD_INSCRICOES"].sum().sort_index()
    print(f"  {'ANO':<8} {'INSCRICOES':>12} {'%':>8}")
    print("  " + "-" * 30)
    for ano, qtd in por_ano.items():
        pct = qtd / total * 100
        print(f"  {ano:<8} {qtd:>12,} {pct:>7.1f}%")


def main():
    print("=" * 70)
    print("  ANALISE TEMPORAL - SEGURO GARANTIA PGFN")
    print("=" * 70)
    print(f"  Diretorio: {config.DATA_DIR}\n")

    # Carregar dados
    df = carregar_dados()

    # Preparar dados
    df = preparar_dados(df)

    if len(df) == 0:
        print("  ERRO: Nenhum registro com data valida.")
        sys.exit(1)

    # Gerar grafico 1: Dispersao diaria
    print("\n" + "-" * 70)
    print("  GRAFICO 1: DISPERSAO DIARIA")
    print("-" * 70)
    fig1 = gerar_dispersao_diaria(df)
    salvar_grafico(fig1, "dispersao_diaria_seguro_garantia.png")
    plt.close(fig1)

    # Gerar grafico 2: Serie mensal
    print("\n" + "-" * 70)
    print("  GRAFICO 2: SERIE MENSAL")
    print("-" * 70)
    fig2, mensal = gerar_serie_mensal(df)
    salvar_grafico(fig2, "serie_mensal_seguro_garantia.png")
    plt.close(fig2)

    # Estatisticas
    imprimir_estatisticas_mensais(mensal)

    # Resumo
    print("\n" + "=" * 70)
    print("  ARQUIVOS GERADOS")
    print("=" * 70)
    print(f"  - {config.get_output_path('dispersao_diaria_seguro_garantia.png')}")
    print(f"  - {config.get_output_path('serie_mensal_seguro_garantia.png')}")

    print("\n" + "=" * 70)
    print("  ANALISE TEMPORAL CONCLUIDA!")
    print("=" * 70)


if __name__ == "__main__":
    main()
