"""
Analise cruzada: TIPO_SITUACAO_INSCRICAO x SITUACAO_INSCRICAO.

Objetivo: Entender a relacao entre essas duas variaveis para calibrar filtros.
Hipoteses testadas:
1. TIPO_SITUACAO_INSCRICAO e uma agregacao de SITUACAO_INSCRICAO?
2. "Em cobranca" = tudo que nao tem garantia?
3. "Garantia" = ATIVA AJUIZADA - GARANTIA - (SG/DEPOSITO/PENHORA/CF)?

Autor: VF Corretora
Data: Janeiro/2026
"""

import pandas as pd
import numpy as np
import sys

import config
from utils import fmt_brl, ler_base_pgfn, converter_valor_brl


def main():
    print("=" * 100)
    print("  ANALISE CRUZADA: TIPO_SITUACAO_INSCRICAO x SITUACAO_INSCRICAO")
    print("=" * 100)

    # Usa a 3a Regiao (SP) como base de referencia
    arquivo = config.BASES[2][0]
    print(f"\nCarregando: {arquivo}")

    try:
        df = ler_base_pgfn(arquivo)
    except FileNotFoundError as e:
        print(f"  ERRO: {e}")
        sys.exit(1)

    print(f"Total de registros: {len(df):,}")

    # Converter valor
    df['VALOR_CONSOLIDADO'] = converter_valor_brl(df['VALOR_CONSOLIDADO'])

    # Filtrar PJ e Ajuizado
    df_pj = df[df['TIPO_PESSOA'].astype(str).str.lower().str.contains('jur', na=False)]
    df_ajuizado = df_pj[df_pj['INDICADOR_AJUIZADO'].astype(str).str.upper() == 'SIM']

    print(f"\nApos filtro PJ + Ajuizado: {len(df_ajuizado):,} registros")

    # ====== ANALISE 1: Distribuicao de TIPO_SITUACAO_INSCRICAO ======
    print("\n" + "=" * 100)
    print("  1. TIPO_SITUACAO_INSCRICAO (PJ + Ajuizado)")
    print("=" * 100)

    tipo_sit = df_ajuizado.groupby('TIPO_SITUACAO_INSCRICAO').agg({
        'NUMERO_INSCRICAO': 'count',
        'CPF_CNPJ': 'nunique',
        'VALOR_CONSOLIDADO': 'sum'
    }).reset_index()
    tipo_sit.columns = ['TIPO_SITUACAO', 'INSCRICOES', 'CNPJS', 'DIVIDA']
    tipo_sit = tipo_sit.sort_values('INSCRICOES', ascending=False)

    total_insc = tipo_sit['INSCRICOES'].sum()
    total_div = tipo_sit['DIVIDA'].sum()

    print(f"\n{'TIPO_SITUACAO':<35} {'INSCR':>12} {'%':>8} {'CNPJs':>10} {'DIVIDA':>18} {'%':>8}")
    print("-" * 95)
    for _, row in tipo_sit.iterrows():
        pct_insc = row['INSCRICOES'] / total_insc * 100
        pct_div = row['DIVIDA'] / total_div * 100 if total_div > 0 else 0
        print(f"{row['TIPO_SITUACAO']:<35} {row['INSCRICOES']:>12,} {pct_insc:>7.1f}% "
              f"{row['CNPJS']:>10,} {fmt_brl(row['DIVIDA']):>18} {pct_div:>7.1f}%")

    # ====== ANALISE 2: Distribuicao de SITUACAO_INSCRICAO ======
    print("\n" + "=" * 100)
    print("  2. SITUACAO_INSCRICAO (PJ + Ajuizado)")
    print("=" * 100)

    sit = df_ajuizado.groupby('SITUACAO_INSCRICAO').agg({
        'NUMERO_INSCRICAO': 'count',
        'CPF_CNPJ': 'nunique',
        'VALOR_CONSOLIDADO': 'sum'
    }).reset_index()
    sit.columns = ['SITUACAO', 'INSCRICOES', 'CNPJS', 'DIVIDA']
    sit = sit.sort_values('INSCRICOES', ascending=False)

    print(f"\n{'SITUACAO':<70} {'INSCR':>10} {'%':>7} {'DIVIDA':>15}")
    print("-" * 105)
    for _, row in sit.iterrows():
        pct = row['INSCRICOES'] / total_insc * 100
        print(f"{str(row['SITUACAO'])[:70]:<70} {row['INSCRICOES']:>10,} {pct:>6.2f}% {fmt_brl(row['DIVIDA']):>15}")

    # ====== ANALISE 3: Cruzamento detalhado ======
    print("\n" + "=" * 100)
    print("  3. CRUZAMENTO: TIPO_SITUACAO -> SITUACAO (detalhes)")
    print("=" * 100)

    for tipo in tipo_sit['TIPO_SITUACAO'].unique():
        subset = df_ajuizado[df_ajuizado['TIPO_SITUACAO_INSCRICAO'] == tipo]
        print(f"\n{'_'*100}")
        print(f"  TIPO_SITUACAO = '{tipo}'")
        print(f"    Total: {len(subset):,} inscricoes | {subset['CPF_CNPJ'].nunique():,} CNPJs | "
              f"{fmt_brl(subset['VALOR_CONSOLIDADO'].sum())}")
        print(f"{'_'*100}")

        det = subset.groupby('SITUACAO_INSCRICAO').agg({
            'NUMERO_INSCRICAO': 'count',
            'VALOR_CONSOLIDADO': 'sum'
        }).reset_index()
        det.columns = ['SITUACAO', 'INSCRICOES', 'DIVIDA']
        det = det.sort_values('INSCRICOES', ascending=False)

        for _, row in det.iterrows():
            pct = row['INSCRICOES'] / len(subset) * 100
            print(f"    - {str(row['SITUACAO'])[:65]:<65} {row['INSCRICOES']:>8,} ({pct:>5.1f}%) {fmt_brl(row['DIVIDA']):>15}")

    # ====== ANALISE 4: Foco no TARGET ======
    print("\n" + "=" * 100)
    print("  4. ANALISE DO TARGET: 'ATIVA AJUIZADA' (sem garantia)")
    print("=" * 100)

    df_ajuizado = df_ajuizado.copy()
    df_ajuizado['TEM_GARANTIA'] = df_ajuizado['SITUACAO_INSCRICAO'].astype(str).str.contains(
        'GARANTIA', case=False, na=False
    )

    com_garantia = df_ajuizado[df_ajuizado['TEM_GARANTIA']]
    sem_garantia = df_ajuizado[~df_ajuizado['TEM_GARANTIA']]

    print(f"\n  COM GARANTIA:")
    print(f"    Inscricoes: {len(com_garantia):,}")
    print(f"    CNPJs: {com_garantia['CPF_CNPJ'].nunique():,}")
    print(f"    Divida: {fmt_brl(com_garantia['VALOR_CONSOLIDADO'].sum())}")

    print(f"\n  SEM GARANTIA:")
    print(f"    Inscricoes: {len(sem_garantia):,}")
    print(f"    CNPJs: {sem_garantia['CPF_CNPJ'].nunique():,}")
    print(f"    Divida: {fmt_brl(sem_garantia['VALOR_CONSOLIDADO'].sum())}")

    # ====== ANALISE 5: Validacao cruzada ======
    print("\n" + "=" * 100)
    print("  5. VALIDACAO: TIPO_SITUACAO='Garantia' vs SITUACAO contem 'GARANTIA'")
    print("=" * 100)

    tipo_garantia = df_ajuizado[df_ajuizado['TIPO_SITUACAO_INSCRICAO'] == 'Garantia']
    sit_garantia = df_ajuizado[df_ajuizado['SITUACAO_INSCRICAO'].astype(str).str.contains(
        'GARANTIA', case=False, na=False
    )]

    print(f"\n    TIPO_SITUACAO = 'Garantia':     {len(tipo_garantia):,} inscricoes")
    print(f"    SITUACAO contem 'GARANTIA':     {len(sit_garantia):,} inscricoes")

    ambos = df_ajuizado[
        (df_ajuizado['TIPO_SITUACAO_INSCRICAO'] == 'Garantia') &
        (df_ajuizado['SITUACAO_INSCRICAO'].astype(str).str.contains('GARANTIA', case=False, na=False))
    ]
    print(f"    Ambos:                          {len(ambos):,} inscricoes")

    # ====== ANALISE 6: Recomendacao de filtro ======
    print("\n" + "=" * 100)
    print("  6. RECOMENDACAO DE FILTRO PARA O ESTUDO")
    print("=" * 100)

    opcao1 = df_ajuizado[df_ajuizado['TIPO_SITUACAO_INSCRICAO'] == 'Em cobranca']
    opcao2 = df_ajuizado[df_ajuizado['SITUACAO_INSCRICAO'] == 'ATIVA AJUIZADA']
    opcao3 = df_ajuizado[~df_ajuizado['SITUACAO_INSCRICAO'].astype(str).str.contains(
        'GARANTIA', case=False, na=False
    )]
    opcao4 = df_ajuizado[
        (df_ajuizado['SITUACAO_INSCRICAO'].astype(str).str.startswith('ATIVA AJUIZADA', na=False)) &
        (~df_ajuizado['SITUACAO_INSCRICAO'].astype(str).str.contains('GARANTIA', case=False, na=False))
    ]

    print(f"\n  Opcao 1 (TIPO='Em cobranca'):   {len(opcao1):>10,} inscricoes | "
          f"{opcao1['CPF_CNPJ'].nunique():>8,} CNPJs | {fmt_brl(opcao1['VALOR_CONSOLIDADO'].sum())}")
    print(f"  Opcao 2 (SIT='ATIVA AJUIZADA'):  {len(opcao2):>10,} inscricoes | "
          f"{opcao2['CPF_CNPJ'].nunique():>8,} CNPJs | {fmt_brl(opcao2['VALOR_CONSOLIDADO'].sum())}")
    print(f"  Opcao 3 (SIT nao GARANTIA):      {len(opcao3):>10,} inscricoes | "
          f"{opcao3['CPF_CNPJ'].nunique():>8,} CNPJs | {fmt_brl(opcao3['VALOR_CONSOLIDADO'].sum())}")
    print(f"  Opcao 4 (ATIVA AJUIZADA s/ GAR): {len(opcao4):>10,} inscricoes | "
          f"{opcao4['CPF_CNPJ'].nunique():>8,} CNPJs | {fmt_brl(opcao4['VALOR_CONSOLIDADO'].sum())}")

    print("\n  RECOMENDACAO: Opcao 4 - mais precisa para o estudo.")

    # ====== ANALISE 7: Detalhamento da opcao 4 ======
    print("\n" + "=" * 100)
    print("  7. DETALHAMENTO DA OPCAO 4 (RECOMENDADA)")
    print("=" * 100)

    det4 = opcao4.groupby('SITUACAO_INSCRICAO').agg({
        'NUMERO_INSCRICAO': 'count',
        'VALOR_CONSOLIDADO': 'sum'
    }).reset_index()
    det4.columns = ['SITUACAO', 'INSCRICOES', 'DIVIDA']
    det4 = det4.sort_values('INSCRICOES', ascending=False)

    print(f"\n{'SITUACAO':<75} {'INSCR':>10} {'DIVIDA':>15}")
    print("-" * 105)
    for _, row in det4.iterrows():
        print(f"{str(row['SITUACAO'])[:75]:<75} {row['INSCRICOES']:>10,} {fmt_brl(row['DIVIDA']):>15}")

    print("\n" + "=" * 100)
    print("  ANALISE CONCLUIDA!")
    print("=" * 100)


if __name__ == "__main__":
    main()
