"""
Pipeline final de consolidacao e classificacao estrategica por CNPJ.

Le a base processada, limpa CPF/CNPJ, consolida por empresa e
classifica por prioridade comercial para prospecao.
"""

import pandas as pd
from datetime import datetime
import sys

import config
from utils import (
    fmt_brl, ler_base_pgfn, converter_valor_brl,
    classificar_estrategico, salvar_csv
)


def main():
    print("  Lendo base original...")

    # Tenta ler a base consolidada; se nao existir, usa a primeira base PGFN
    try:
        entrada = config.get_data_path("amostra_grande_pgfn.csv")
        df = pd.read_csv(entrada, sep=";", low_memory=False)
    except FileNotFoundError:
        print("  amostra_grande_pgfn.csv nao encontrado, usando base SIDA_1...")
        try:
            df = ler_base_pgfn(config.BASES[0][0], colunas=config.COLUNAS_COMPLETAS)
        except FileNotFoundError as e:
            print(f"  ERRO: {e}")
            sys.exit(1)

    print(f"  Linhas lidas: {len(df):,}")

    # ========== LIMPEZA DE DOCUMENTO ==========
    print("  Limpando CPF/CNPJ...")
    df["DOC_LIMPO"] = (
        df["CPF_CNPJ"].astype(str)
        .str.replace(r"\D", "", regex=True)
    )

    # Apenas CNPJ real (14 digitos)
    df = df[df["DOC_LIMPO"].str.len() == 14]
    print(f"  Linhas apos filtro CNPJ: {len(df):,}")
    print(f"  CNPJs distintos: {df['DOC_LIMPO'].nunique():,}")

    # ========== TRATAMENTO DE VALOR ==========
    print("  Tratando VALOR_CONSOLIDADO...")
    df["VALOR_CONSOLIDADO"] = converter_valor_brl(df["VALOR_CONSOLIDADO"])
    df = df[df["VALOR_CONSOLIDADO"].notna()]

    # ========== DATAS E FLAGS ==========
    print("  Processando datas e flags juridicas...")
    df["DATA_INSCRICAO"] = pd.to_datetime(df["DATA_INSCRICAO"], errors="coerce", dayfirst=True)
    hoje = pd.Timestamp(datetime.today().date())
    df["IDADE_DIAS"] = (hoje - df["DATA_INSCRICAO"]).dt.days

    df["FLAG_AJUIZADA"] = (
        df["INDICADOR_AJUIZADO"].astype(str).str.upper().isin(["SIM", "S"])
    )

    df["FLAG_EM_COBRANCA"] = (
        df["SITUACAO_INSCRICAO"].astype(str).str.upper().str.contains("COBRANCA", na=False)
    )

    # ========== CONSOLIDACAO POR CNPJ ==========
    print("  Consolidando por CNPJ...")
    consolidado = (
        df.groupby("DOC_LIMPO")
        .agg(
            VALOR_TOTAL=("VALOR_CONSOLIDADO", "sum"),
            QTD_INSCRICOES=("VALOR_CONSOLIDADO", "count"),
            DIVIDA_MAIS_ANTIGA=("IDADE_DIAS", "max"),
            DIVIDA_MAIS_RECENTE=("IDADE_DIAS", "min"),
            TEM_AJUIZAMENTO=("FLAG_AJUIZADA", "any"),
            TEM_EM_COBRANCA=("FLAG_EM_COBRANCA", "any"),
        )
        .reset_index()
    )

    # ========== CLASSIFICACAO ESTRATEGICA ==========
    print("  Classificando empresas...")
    consolidado["CLASSIFICACAO"] = consolidado.apply(classificar_estrategico, axis=1)

    # ========== RELATORIOS ==========
    print("\n  Distribuicao por classificacao:")
    class_dist = consolidado["CLASSIFICACAO"].value_counts()
    for classif, qtd in class_dist.items():
        pct = qtd / len(consolidado) * 100
        print(f"    {classif}: {qtd:,} ({pct:.1f}%)")

    print("\n  Top 10 maiores passivos:")
    top10 = consolidado.sort_values("VALOR_TOTAL", ascending=False).head(10)
    print(f"  {'CNPJ':<20} {'VALOR TOTAL':>20} {'INSCR':>8} {'CLASSIF':<40}")
    print("  " + "-" * 90)
    for _, row in top10.iterrows():
        print(f"  {row['DOC_LIMPO']:<20} {fmt_brl(row['VALOR_TOTAL']):>20} "
              f"{row['QTD_INSCRICOES']:>8} {row['CLASSIFICACAO']:<40}")

    # ========== EXPORTACAO ==========
    salvar_csv(consolidado, "cnpj_consolidado_classificado_final.csv")

    print(f"\n  PIPELINE FINAL CONCLUIDO")
    print(f"  Empresas consolidadas: {len(consolidado):,}")


if __name__ == "__main__":
    main()
