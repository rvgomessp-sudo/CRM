"""
Extracao de inscricoes com Seguro Garantia - PGFN

Consolida TODAS as inscricoes com SEGURO GARANTIA das 6 bases regionais PGFN
em um unico arquivo CSV, preservando todas as colunas originais.

Gera:
- pgfn_seguro_garantia_todas_inscricoes.csv (dados consolidados)
- pgfn_histograma_seguro_garantia.png (distribuicao temporal)
"""

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import sys

import config
from utils import ler_base_pgfn, converter_valor_brl, salvar_grafico


def carregar_e_filtrar_seguro_garantia(arquivo, regiao):
    """Carrega uma base e filtra apenas inscricoes com Seguro Garantia.

    Preserva TODAS as colunas originais, sem filtros adicionais.
    """
    print(f"  Lendo {regiao}...")

    try:
        # Ler TODAS as colunas (sem filtro de colunas)
        df = ler_base_pgfn(arquivo, colunas=None)
    except FileNotFoundError as e:
        print(f"    AVISO: {e}")
        return None

    total_bruto = len(df)
    print(f"    Total bruto: {total_bruto:,}")

    # Filtrar APENAS inscricoes com SEGURO GARANTIA
    # Nota: SITUACAO_INSCRICAO contem descricoes como "ATIVA AJUIZADA - COM GARANTIA - SEGURO GARANTIA"
    # Usamos str.contains para capturar todas as variantes
    mask_sg = df["SITUACAO_INSCRICAO"].astype(str).str.upper().str.contains("SEGURO GARANTIA", na=False)
    df_sg = df[mask_sg].copy()

    print(f"    Com Seguro Garantia: {len(df_sg):,} ({len(df_sg)/total_bruto*100:.2f}%)")

    # Adicionar coluna de regiao para referencia
    df_sg["REGIAO_PGFN"] = regiao

    return df_sg


def processar_valores_e_datas(df):
    """Converte VALOR_CONSOLIDADO para numerico e DATA_INSCRICAO para datetime."""
    df = df.copy()

    # Converter valor (formato brasileiro: 1.234.567,89 -> float)
    df["VALOR_CONSOLIDADO"] = converter_valor_brl(df["VALOR_CONSOLIDADO"])

    # Converter data
    df["DATA_INSCRICAO"] = pd.to_datetime(
        df["DATA_INSCRICAO"],
        errors="coerce",
        dayfirst=True
    )

    # Extrair ano para analise temporal
    df["ANO_INSCRICAO"] = df["DATA_INSCRICAO"].dt.year

    return df


def gerar_analise_temporal(df):
    """Gera tabela de frequencia e histograma por ano."""

    # Tabela de frequencia por ano
    freq_ano = df.groupby("ANO_INSCRICAO").agg(
        QTD_INSCRICOES=("NUMERO_INSCRICAO", "count"),
        VALOR_TOTAL=("VALOR_CONSOLIDADO", "sum"),
        VALOR_MEDIO=("VALOR_CONSOLIDADO", "mean"),
        CNPJs_DISTINTOS=("CPF_CNPJ", "nunique"),
    ).reset_index()

    freq_ano = freq_ano.sort_values("ANO_INSCRICAO")

    # Remover anos invalidos (NaN)
    freq_ano = freq_ano[freq_ano["ANO_INSCRICAO"].notna()]
    freq_ano["ANO_INSCRICAO"] = freq_ano["ANO_INSCRICAO"].astype(int)

    return freq_ano


def gerar_histograma(freq_ano, total_inscricoes):
    """Gera histograma de inscricoes com Seguro Garantia por ano."""

    fig, ax = plt.subplots(figsize=(14, 7))

    anos = freq_ano["ANO_INSCRICAO"].values
    qtds = freq_ano["QTD_INSCRICOES"].values

    # Barras
    bars = ax.bar(anos, qtds, color="#27ae60", edgecolor="white", width=0.8)

    # Adicionar valores nas barras
    for bar, qtd in zip(bars, qtds):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, height + max(qtds)*0.01,
                f"{int(qtd):,}", ha="center", va="bottom", fontsize=8, rotation=0)

    # Formatacao
    ax.set_xlabel("Ano de Inscricao", fontsize=11)
    ax.set_ylabel("Quantidade de Inscricoes com Seguro Garantia", fontsize=11)
    ax.set_title(f"Distribuicao Temporal de Inscricoes com Seguro Garantia - PGFN\n"
                 f"Total: {total_inscricoes:,} inscricoes", fontsize=13, fontweight="bold")

    # Ajustar eixo X para mostrar todos os anos
    ax.set_xticks(anos)
    ax.set_xticklabels(anos, rotation=45, ha="right", fontsize=9)

    # Grid
    ax.grid(axis="y", alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    # Remover bordas superiores e direitas
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    return fig


def main():
    print("=" * 80)
    print("  EXTRACAO DE INSCRICOES COM SEGURO GARANTIA - PGFN")
    print("=" * 80)
    print(f"  Diretorio de dados: {config.DATA_DIR}\n")

    # Processar todas as 6 bases
    partes = []
    for arquivo, nome_curto, nome_completo in config.BASES:
        df_sg = carregar_e_filtrar_seguro_garantia(arquivo, nome_completo)
        if df_sg is not None and len(df_sg) > 0:
            partes.append(df_sg)

    if not partes:
        print("\nERRO: Nenhuma inscricao com Seguro Garantia encontrada nas bases.")
        sys.exit(1)

    # Consolidar
    print("\n" + "-" * 80)
    print("  CONSOLIDANDO...")
    df_consolidado = pd.concat(partes, ignore_index=True)
    print(f"  Total consolidado: {len(df_consolidado):,} inscricoes com Seguro Garantia")
    print(f"  CNPJs distintos: {df_consolidado['CPF_CNPJ'].nunique():,}")

    # Processar valores e datas
    print("\n  Convertendo valores e datas...")
    df_consolidado = processar_valores_e_datas(df_consolidado)

    valor_total = df_consolidado["VALOR_CONSOLIDADO"].sum()
    print(f"  Valor total consolidado: R$ {valor_total:,.2f}")

    # Salvar CSV consolidado (sem notacao cientifica)
    print("\n" + "-" * 80)
    print("  SALVANDO CSV...")

    csv_path = config.get_output_path("pgfn_seguro_garantia_todas_inscricoes.csv")

    # Criar copia para exportacao, formatando VALOR_CONSOLIDADO sem notacao cientifica
    df_export = df_consolidado.copy()
    df_export["VALOR_CONSOLIDADO"] = df_export["VALOR_CONSOLIDADO"].apply(
        lambda x: f"{x:.2f}" if pd.notna(x) else ""
    )

    df_export.to_csv(csv_path, sep=";", index=False, encoding="utf-8-sig")
    print(f"  CSV salvo: {csv_path}")
    print(f"  Registros: {len(df_consolidado):,}")

    # Analise temporal
    print("\n" + "-" * 80)
    print("  ANALISE TEMPORAL...")

    freq_ano = gerar_analise_temporal(df_consolidado)

    print(f"\n  DISTRIBUICAO POR ANO DE INSCRICAO:\n")
    print(f"  {'ANO':<8} {'INSCRICOES':>12} {'CNPJs':>10} {'VALOR TOTAL':>20} {'VALOR MEDIO':>18}")
    print("  " + "-" * 70)

    for _, row in freq_ano.iterrows():
        ano = int(row["ANO_INSCRICAO"])
        qtd = int(row["QTD_INSCRICOES"])
        cnpjs = int(row["CNPJs_DISTINTOS"])
        val_total = row["VALOR_TOTAL"]
        val_medio = row["VALOR_MEDIO"]

        # Formatar valores
        if val_total >= 1e9:
            val_total_str = f"R$ {val_total/1e9:.2f} bi"
        elif val_total >= 1e6:
            val_total_str = f"R$ {val_total/1e6:.2f} mi"
        else:
            val_total_str = f"R$ {val_total:,.2f}"

        if val_medio >= 1e6:
            val_medio_str = f"R$ {val_medio/1e6:.2f} mi"
        else:
            val_medio_str = f"R$ {val_medio:,.2f}"

        print(f"  {ano:<8} {qtd:>12,} {cnpjs:>10,} {val_total_str:>20} {val_medio_str:>18}")

    print("  " + "-" * 70)
    print(f"  {'TOTAL':<8} {freq_ano['QTD_INSCRICOES'].sum():>12,} "
          f"{df_consolidado['CPF_CNPJ'].nunique():>10,}")

    # Gerar histograma
    print("\n" + "-" * 80)
    print("  GERANDO HISTOGRAMA...")

    fig = gerar_histograma(freq_ano, len(df_consolidado))
    salvar_grafico(fig, "pgfn_histograma_seguro_garantia.png")
    plt.close(fig)

    # Resumo final
    print("\n" + "=" * 80)
    print("  RESUMO FINAL")
    print("=" * 80)
    print(f"\n  Inscricoes com Seguro Garantia: {len(df_consolidado):,}")
    print(f"  CNPJs distintos: {df_consolidado['CPF_CNPJ'].nunique():,}")
    print(f"  Valor total segurado: R$ {valor_total:,.2f}")
    print(f"  Valor medio por inscricao: R$ {valor_total/len(df_consolidado):,.2f}")

    # Periodo
    ano_min = int(freq_ano["ANO_INSCRICAO"].min())
    ano_max = int(freq_ano["ANO_INSCRICAO"].max())
    print(f"  Periodo: {ano_min} a {ano_max}")

    # Arquivos gerados
    print(f"\n  ARQUIVOS GERADOS:")
    print(f"    - {csv_path}")
    print(f"    - {config.get_output_path('pgfn_histograma_seguro_garantia.png')}")

    print("\n" + "=" * 80)
    print("  EXTRACAO CONCLUIDA!")
    print("=" * 80)


if __name__ == "__main__":
    main()
