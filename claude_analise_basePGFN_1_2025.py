"""
Analise rapida da base PGFN SIDA_1 - Visao geral exploratoria.

Gera amostra, estatisticas descritivas e resumo executivo
para exploracao inicial dos dados antes das analises detalhadas.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys

import config
from utils import (
    fmt_brl, ler_base_pgfn, converter_valor_brl, filtrar_pj,
    filtrar_tributos, adicionar_features, salvar_csv, salvar_grafico
)


def main():
    arquivo = config.BASES[0][0]
    print("=" * 80)
    print("  ANALISE EXPLORATORIA - BASE PGFN SIDA_1")
    print("=" * 80)

    # Verificar arquivos disponiveis
    print(f"\n  Diretorio de dados: {config.DATA_DIR}")
    if config.DATA_DIR.exists():
        arquivos = list(config.DATA_DIR.glob("*.csv"))
        print(f"  Arquivos CSV encontrados: {len(arquivos)}")
        for f in arquivos[:10]:
            print(f"    - {f.name}")
    else:
        print(f"  AVISO: Diretorio nao encontrado: {config.DATA_DIR}")

    # Ler base
    print(f"\n  Lendo {arquivo}...")
    try:
        df = ler_base_pgfn(arquivo, colunas=config.COLUNAS_COMPLETAS)
    except FileNotFoundError as e:
        print(f"  ERRO: {e}")
        print("  Verifique se o diretorio de dados esta configurado corretamente em config.py")
        print("  Ou defina a variavel de ambiente PGFN_DATA_DIR")
        sys.exit(1)

    print(f"  Total de registros: {len(df):,}")

    # ========== ESTATISTICAS BASICAS ==========
    print("\n" + "=" * 80)
    print("  ESTATISTICAS BASICAS")
    print("=" * 80)

    print(f"\n  Colunas: {list(df.columns)}")
    print(f"\n  Shape: {df.shape}")
    print(f"\n  Tipos de dados:")
    for col in df.columns:
        print(f"    {col}: {df[col].dtype} (nulos: {df[col].isna().sum():,})")

    # Distribuicao de TIPO_PESSOA
    print(f"\n  TIPO_PESSOA:")
    print(df["TIPO_PESSOA"].value_counts().to_string())

    # ========== APLICAR FILTROS ==========
    print("\n" + "=" * 80)
    print("  APLICANDO FILTROS PADRAO")
    print("=" * 80)

    df_pj = filtrar_pj(df)
    print(f"  Apos filtro PJ: {len(df_pj):,}")

    df_filtrado = filtrar_tributos(df_pj)
    print(f"  Apos filtro tributos: {len(df_filtrado):,}")

    # Converter valor
    df_filtrado = df_filtrado.copy()
    df_filtrado["VALOR_CONSOLIDADO"] = converter_valor_brl(df_filtrado["VALOR_CONSOLIDADO"])

    # Adicionar features
    df_filtrado = adicionar_features(df_filtrado)

    # ========== RESUMO POS-FILTRO ==========
    print("\n" + "=" * 80)
    print("  RESUMO POS-FILTRO")
    print("=" * 80)

    total = len(df_filtrado)
    cnpjs = df_filtrado["CPF_CNPJ"].nunique()
    divida = df_filtrado["VALOR_CONSOLIDADO"].sum()
    media = df_filtrado["VALOR_CONSOLIDADO"].mean()
    mediana = df_filtrado["VALOR_CONSOLIDADO"].median()

    print(f"\n  Inscricoes: {total:,}")
    print(f"  CNPJs unicos: {cnpjs:,}")
    print(f"  Divida total: {fmt_brl(divida)}")
    print(f"  Divida media: {fmt_brl(media)}")
    print(f"  Divida mediana: {fmt_brl(mediana)}")

    # Por ajuizamento
    ajuiz = df_filtrado.groupby("AJUIZADO_BIN").agg(
        QTD=("NUMERO_INSCRICAO", "count"),
        DIVIDA=("VALOR_CONSOLIDADO", "sum"),
        CNPJS=("CPF_CNPJ", "nunique"),
    )
    print(f"\n  AJUIZAMENTO:")
    for idx, row in ajuiz.iterrows():
        status = "AJUIZADO" if idx else "NAO AJUIZADO"
        print(f"    {status}: {int(row['QTD']):,} inscricoes | "
              f"{int(row['CNPJS']):,} CNPJs | {fmt_brl(row['DIVIDA'])}")

    # Por garantia
    gar = df_filtrado.groupby("GARANTIA_ATUAL").agg(
        QTD=("NUMERO_INSCRICAO", "count"),
        DIVIDA=("VALOR_CONSOLIDADO", "sum"),
    ).sort_values("QTD", ascending=False)
    print(f"\n  GARANTIA:")
    for idx, row in gar.iterrows():
        pct = row['QTD'] / total * 100
        print(f"    {idx}: {int(row['QTD']):,} ({pct:.1f}%) | {fmt_brl(row['DIVIDA'])}")

    # Por safra
    if "SAFRA" in df_filtrado.columns:
        safra = df_filtrado.groupby("SAFRA").agg(
            QTD=("NUMERO_INSCRICAO", "count"),
            DIVIDA=("VALOR_CONSOLIDADO", "sum"),
        ).sort_index()
        print(f"\n  SAFRA:")
        for idx, row in safra.iterrows():
            print(f"    {idx}: {int(row['QTD']):,} inscricoes | {fmt_brl(row['DIVIDA'])}")

    # Por UF
    uf = df_filtrado.groupby("UF_DEVEDOR").agg(
        QTD=("NUMERO_INSCRICAO", "count"),
        CNPJS=("CPF_CNPJ", "nunique"),
        DIVIDA=("VALOR_CONSOLIDADO", "sum"),
    ).sort_values("DIVIDA", ascending=False)
    print(f"\n  TOP 10 UFs (por divida):")
    for idx, row in uf.head(10).iterrows():
        print(f"    {idx}: {int(row['QTD']):,} inscricoes | {int(row['CNPJS']):,} CNPJs | {fmt_brl(row['DIVIDA'])}")

    # ========== GRAFICOS EXPLORATORIOS ==========
    print("\n  Gerando graficos exploratorios...")
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # 1. Histograma de valores (log)
    ax1 = axes[0, 0]
    valores_positivos = df_filtrado["VALOR_CONSOLIDADO"].clip(lower=1)
    ax1.hist(np.log10(valores_positivos), bins=50, color='#3498db', alpha=0.7, edgecolor='white')
    ax1.axvline(np.log10(1_000_000), color='red', linestyle='--', label='R$ 1 mi')
    ax1.set_title("Distribuicao de Valores (log10)", fontweight='bold')
    ax1.set_xlabel("log10(Valor)")
    ax1.set_ylabel("Frequencia")
    ax1.legend()

    # 2. Por garantia
    ax2 = axes[0, 1]
    gar_plot = gar["QTD"].sort_values(ascending=True)
    ax2.barh(gar_plot.index, gar_plot.values, color='#2ecc71')
    ax2.set_title("Inscricoes por Tipo de Garantia", fontweight='bold')
    ax2.set_xlabel("Quantidade")

    # 3. Por UF (top 10)
    ax3 = axes[1, 0]
    uf_top = uf.head(10)["QTD"].sort_values(ascending=True)
    ax3.barh(uf_top.index, uf_top.values, color='#e74c3c')
    ax3.set_title("Top 10 UFs (por inscricoes)", fontweight='bold')
    ax3.set_xlabel("Quantidade")

    # 4. Ajuizado vs Nao ajuizado
    ax4 = axes[1, 1]
    ajuiz_labels = ["Ajuizado", "Nao Ajuizado"]
    ajuiz_vals = [
        ajuiz.loc[True, "QTD"] if True in ajuiz.index else 0,
        ajuiz.loc[False, "QTD"] if False in ajuiz.index else 0,
    ]
    ax4.pie(ajuiz_vals, labels=ajuiz_labels, autopct='%1.1f%%',
            colors=['#e74c3c', '#3498db'], startangle=90)
    ax4.set_title("Ajuizado vs Nao Ajuizado", fontweight='bold')

    plt.suptitle(f"ANALISE EXPLORATORIA - {arquivo}", fontsize=13, fontweight='bold', y=1.02)
    plt.tight_layout()
    salvar_grafico(fig, "analise_exploratoria_sida1.png")
    plt.close(fig)

    # ========== EXPORTAR AMOSTRA ==========
    amostra = df_filtrado.head(100_000)
    salvar_csv(amostra, "amostra_100k.csv")
    print(f"  Amostra salva: {len(amostra):,} linhas")

    # ========== MERCADO ALVO ==========
    mercado = df_filtrado[
        (df_filtrado["AJUIZADO_BIN"]) &
        (df_filtrado["GARANTIA_ATUAL"] == "SEM GARANTIA")
    ]

    print("\n" + "=" * 80)
    print("  MERCADO ALVO: AJUIZADO + SEM GARANTIA")
    print("=" * 80)
    print(f"  Inscricoes: {len(mercado):,}")
    print(f"  CNPJs: {mercado['CPF_CNPJ'].nunique():,}")
    print(f"  Divida: {fmt_brl(mercado['VALOR_CONSOLIDADO'].sum())}")

    from utils import calcular_potencial_comercial
    premio, comissao = calcular_potencial_comercial(mercado['VALOR_CONSOLIDADO'].sum())
    print(f"  Premio potencial (2%): {fmt_brl(premio)}")
    print(f"  Comissao potencial (25%): {fmt_brl(comissao)}")

    print("\n" + "=" * 80)
    print("  ANALISE EXPLORATORIA CONCLUIDA!")
    print("=" * 80)


if __name__ == "__main__":
    main()
