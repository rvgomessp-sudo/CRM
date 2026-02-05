"""
================================================================================
ANÁLISE CRUZADA: TIPO_SITUACAO_INSCRICAO x SITUACAO_INSCRICAO
================================================================================
Objetivo: Entender a relação entre essas duas variáveis para calibrar filtros

Hipóteses a testar:
1. TIPO_SITUACAO_INSCRICAO é uma agregação de SITUACAO_INSCRICAO?
2. "Em cobrança" = tudo que não tem garantia?
3. "Garantia" = ATIVA AJUIZADA - GARANTIA - (SG/DEPOSITO/PENHORA/CF)?

Autor: VF Corretora
Data: Janeiro/2026
================================================================================
"""

import pandas as pd
import numpy as np
import os

# ================= CONFIGURAÇÃO =================
PGFN_DIR = r"C:\Rodrigo\BasePGFN\2025"

# Usar a base maior (3ª Região - SP)
ARQUIVO = "arquivo_lai_SIDA_3_202512.csv"


def fmt_brl(valor):
    if pd.isna(valor): return "R$ -"
    if valor >= 1e12: return f"R$ {valor/1e12:.2f} tri"
    if valor >= 1e9: return f"R$ {valor/1e9:.2f} bi"
    if valor >= 1e6: return f"R$ {valor/1e6:.2f} mi"
    return f"R$ {valor:,.0f}"


def main():
    print("=" * 100)
    print("🔍 ANÁLISE CRUZADA: TIPO_SITUACAO_INSCRICAO x SITUACAO_INSCRICAO")
    print("=" * 100)
    
    # Carregar base
    caminho = os.path.join(PGFN_DIR, ARQUIVO)
    print(f"\nCarregando: {ARQUIVO}")
    
    df = pd.read_csv(caminho, sep=";", encoding="latin1", dtype=str)
    print(f"Total de registros: {len(df):,}")
    
    # Converter valor
    df['VALOR_CONSOLIDADO'] = (
        df['VALOR_CONSOLIDADO']
        .str.replace('.', '', regex=False)
        .str.replace(',', '.', regex=False)
        .astype(float)
    )
    
    # Filtrar apenas PJ e Ajuizado para focar no público-alvo
    df_pj = df[df['TIPO_PESSOA'].str.lower().str.contains('jur', na=False)]
    df_ajuizado = df_pj[df_pj['INDICADOR_AJUIZADO'].str.upper() == 'SIM']
    
    print(f"\nApós filtro PJ + Ajuizado: {len(df_ajuizado):,} registros")
    
    # =========================================================================
    # ANÁLISE 1: Distribuição de TIPO_SITUACAO_INSCRICAO
    # =========================================================================
    print("\n" + "=" * 100)
    print("📊 1. TIPO_SITUACAO_INSCRICAO (PJ + Ajuizado)")
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
    
    print(f"\n{'TIPO_SITUACAO':<35} {'INSCR':>12} {'%':>8} {'CNPJs':>10} {'DÍVIDA':>18} {'%':>8}")
    print("-" * 95)
    
    for _, row in tipo_sit.iterrows():
        pct_insc = row['INSCRICOES'] / total_insc * 100
        pct_div = row['DIVIDA'] / total_div * 100
        print(f"{row['TIPO_SITUACAO']:<35} {row['INSCRICOES']:>12,} {pct_insc:>7.1f}% {row['CNPJS']:>10,} {fmt_brl(row['DIVIDA']):>18} {pct_div:>7.1f}%")
    
    # =========================================================================
    # ANÁLISE 2: Distribuição de SITUACAO_INSCRICAO
    # =========================================================================
    print("\n" + "=" * 100)
    print("📊 2. SITUACAO_INSCRICAO (PJ + Ajuizado)")
    print("=" * 100)
    
    sit = df_ajuizado.groupby('SITUACAO_INSCRICAO').agg({
        'NUMERO_INSCRICAO': 'count',
        'CPF_CNPJ': 'nunique',
        'VALOR_CONSOLIDADO': 'sum'
    }).reset_index()
    sit.columns = ['SITUACAO', 'INSCRICOES', 'CNPJS', 'DIVIDA']
    sit = sit.sort_values('INSCRICOES', ascending=False)
    
    print(f"\n{'SITUACAO':<70} {'INSCR':>10} {'%':>7} {'DÍVIDA':>15}")
    print("-" * 105)
    
    for _, row in sit.iterrows():
        pct = row['INSCRICOES'] / total_insc * 100
        print(f"{row['SITUACAO'][:70]:<70} {row['INSCRICOES']:>10,} {pct:>6.2f}% {fmt_brl(row['DIVIDA']):>15}")
    
    # =========================================================================
    # ANÁLISE 3: CRUZAMENTO - Para cada TIPO_SITUACAO, quais SITUACAO existem?
    # =========================================================================
    print("\n" + "=" * 100)
    print("📊 3. CRUZAMENTO: TIPO_SITUACAO → SITUACAO (detalhes)")
    print("=" * 100)
    
    for tipo in tipo_sit['TIPO_SITUACAO'].unique():
        subset = df_ajuizado[df_ajuizado['TIPO_SITUACAO_INSCRICAO'] == tipo]
        
        print(f"\n{'─'*100}")
        print(f"📌 TIPO_SITUACAO = '{tipo}'")
        print(f"   Total: {len(subset):,} inscrições | {subset['CPF_CNPJ'].nunique():,} CNPJs | {fmt_brl(subset['VALOR_CONSOLIDADO'].sum())}")
        print(f"{'─'*100}")
        
        # Detalhamento por SITUACAO
        det = subset.groupby('SITUACAO_INSCRICAO').agg({
            'NUMERO_INSCRICAO': 'count',
            'VALOR_CONSOLIDADO': 'sum'
        }).reset_index()
        det.columns = ['SITUACAO', 'INSCRICOES', 'DIVIDA']
        det = det.sort_values('INSCRICOES', ascending=False)
        
        for _, row in det.iterrows():
            pct = row['INSCRICOES'] / len(subset) * 100
            print(f"   • {row['SITUACAO'][:65]:<65} {row['INSCRICOES']:>8,} ({pct:>5.1f}%) {fmt_brl(row['DIVIDA']):>15}")
    
    # =========================================================================
    # ANÁLISE 4: Foco no TARGET - "ATIVA AJUIZADA" sem garantia
    # =========================================================================
    print("\n" + "=" * 100)
    print("📊 4. ANÁLISE DO TARGET: 'ATIVA AJUIZADA' (sem garantia)")
    print("=" * 100)
    
    # Identificar situações com garantia
    df_ajuizado['TEM_GARANTIA'] = df_ajuizado['SITUACAO_INSCRICAO'].str.contains('GARANTIA', case=False, na=False)
    
    com_garantia = df_ajuizado[df_ajuizado['TEM_GARANTIA']]
    sem_garantia = df_ajuizado[~df_ajuizado['TEM_GARANTIA']]
    
    print(f"\n📌 COM GARANTIA (no nome):")
    print(f"   Inscrições: {len(com_garantia):,}")
    print(f"   CNPJs: {com_garantia['CPF_CNPJ'].nunique():,}")
    print(f"   Dívida: {fmt_brl(com_garantia['VALOR_CONSOLIDADO'].sum())}")
    
    print(f"\n📌 SEM GARANTIA (no nome):")
    print(f"   Inscrições: {len(sem_garantia):,}")
    print(f"   CNPJs: {sem_garantia['CPF_CNPJ'].nunique():,}")
    print(f"   Dívida: {fmt_brl(sem_garantia['VALOR_CONSOLIDADO'].sum())}")
    
    # Detalhamento das garantias
    print(f"\n📌 TIPOS DE GARANTIA encontrados:")
    garantias = com_garantia.groupby('SITUACAO_INSCRICAO').agg({
        'NUMERO_INSCRICAO': 'count',
        'VALOR_CONSOLIDADO': 'sum'
    }).reset_index()
    garantias = garantias.sort_values('VALOR_CONSOLIDADO', ascending=False)
    
    for _, row in garantias.iterrows():
        print(f"   • {row['SITUACAO_INSCRICAO']:<60} {row['NUMERO_INSCRICAO']:>8,} inscr | {fmt_brl(row['VALOR_CONSOLIDADO']):>15}")
    
    # =========================================================================
    # ANÁLISE 5: Comparar TIPO_SITUACAO = "Garantia" vs SITUACAO contém "GARANTIA"
    # =========================================================================
    print("\n" + "=" * 100)
    print("📊 5. VALIDAÇÃO: TIPO_SITUACAO='Garantia' vs SITUACAO contém 'GARANTIA'")
    print("=" * 100)
    
    tipo_garantia = df_ajuizado[df_ajuizado['TIPO_SITUACAO_INSCRICAO'] == 'Garantia']
    sit_garantia = df_ajuizado[df_ajuizado['SITUACAO_INSCRICAO'].str.contains('GARANTIA', case=False, na=False)]
    
    print(f"\n   TIPO_SITUACAO = 'Garantia':     {len(tipo_garantia):,} inscrições")
    print(f"   SITUACAO contém 'GARANTIA':     {len(sit_garantia):,} inscrições")
    
    # Interseção
    ambos = df_ajuizado[
        (df_ajuizado['TIPO_SITUACAO_INSCRICAO'] == 'Garantia') & 
        (df_ajuizado['SITUACAO_INSCRICAO'].str.contains('GARANTIA', case=False, na=False))
    ]
    print(f"   Ambos:                          {len(ambos):,} inscrições")
    
    # Diferenças
    tipo_mas_nao_sit = df_ajuizado[
        (df_ajuizado['TIPO_SITUACAO_INSCRICAO'] == 'Garantia') & 
        (~df_ajuizado['SITUACAO_INSCRICAO'].str.contains('GARANTIA', case=False, na=False))
    ]
    sit_mas_nao_tipo = df_ajuizado[
        (df_ajuizado['TIPO_SITUACAO_INSCRICAO'] != 'Garantia') & 
        (df_ajuizado['SITUACAO_INSCRICAO'].str.contains('GARANTIA', case=False, na=False))
    ]
    
    print(f"   TIPO='Garantia' mas SIT não:    {len(tipo_mas_nao_sit):,} inscrições")
    print(f"   SIT='GARANTIA' mas TIPO não:    {len(sit_mas_nao_tipo):,} inscrições")
    
    if len(tipo_mas_nao_sit) > 0:
        print(f"\n   ⚠️ Casos com TIPO='Garantia' mas SITUACAO não contém 'GARANTIA':")
        for sit in tipo_mas_nao_sit['SITUACAO_INSCRICAO'].unique()[:10]:
            count = len(tipo_mas_nao_sit[tipo_mas_nao_sit['SITUACAO_INSCRICAO'] == sit])
            print(f"      • {sit}: {count:,}")
    
    if len(sit_mas_nao_tipo) > 0:
        print(f"\n   ⚠️ Casos com SITUACAO contém 'GARANTIA' mas TIPO não é 'Garantia':")
        for sit in sit_mas_nao_tipo['SITUACAO_INSCRICAO'].unique()[:10]:
            count = len(sit_mas_nao_tipo[sit_mas_nao_tipo['SITUACAO_INSCRICAO'] == sit])
            tipo = sit_mas_nao_tipo[sit_mas_nao_tipo['SITUACAO_INSCRICAO'] == sit]['TIPO_SITUACAO_INSCRICAO'].iloc[0]
            print(f"      • {sit}: {count:,} (TIPO={tipo})")
    
    # =========================================================================
    # ANÁLISE 6: RECOMENDAÇÃO DE FILTRO
    # =========================================================================
    print("\n" + "=" * 100)
    print("🎯 6. RECOMENDAÇÃO DE FILTRO PARA O ESTUDO")
    print("=" * 100)
    
    # Opção 1: Usar TIPO_SITUACAO = "Em cobrança"
    opcao1 = df_ajuizado[df_ajuizado['TIPO_SITUACAO_INSCRICAO'] == 'Em cobrança']
    
    # Opção 2: Usar SITUACAO = "ATIVA AJUIZADA" (exata)
    opcao2 = df_ajuizado[df_ajuizado['SITUACAO_INSCRICAO'] == 'ATIVA AJUIZADA']
    
    # Opção 3: Usar SITUACAO não contém "GARANTIA"
    opcao3 = df_ajuizado[~df_ajuizado['SITUACAO_INSCRICAO'].str.contains('GARANTIA', case=False, na=False)]
    
    # Opção 4: SITUACAO começa com "ATIVA AJUIZADA" e não contém "GARANTIA"
    opcao4 = df_ajuizado[
        (df_ajuizado['SITUACAO_INSCRICAO'].str.startswith('ATIVA AJUIZADA', na=False)) &
        (~df_ajuizado['SITUACAO_INSCRICAO'].str.contains('GARANTIA', case=False, na=False))
    ]
    
    print(f"""
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│  OPÇÕES DE FILTRO PARA "SEM GARANTIA"                                                   │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│  Opção 1: TIPO_SITUACAO = 'Em cobrança'                                                 │
│           → {len(opcao1):>10,} inscrições | {opcao1['CPF_CNPJ'].nunique():>8,} CNPJs | {fmt_brl(opcao1['VALOR_CONSOLIDADO'].sum()):>18}     │
│                                                                                         │
│  Opção 2: SITUACAO = 'ATIVA AJUIZADA' (exata)                                          │
│           → {len(opcao2):>10,} inscrições | {opcao2['CPF_CNPJ'].nunique():>8,} CNPJs | {fmt_brl(opcao2['VALOR_CONSOLIDADO'].sum()):>18}     │
│                                                                                         │
│  Opção 3: SITUACAO não contém 'GARANTIA'                                               │
│           → {len(opcao3):>10,} inscrições | {opcao3['CPF_CNPJ'].nunique():>8,} CNPJs | {fmt_brl(opcao3['VALOR_CONSOLIDADO'].sum()):>18}     │
│                                                                                         │
│  Opção 4: SITUACAO começa com 'ATIVA AJUIZADA' e não contém 'GARANTIA'                 │
│           → {len(opcao4):>10,} inscrições | {opcao4['CPF_CNPJ'].nunique():>8,} CNPJs | {fmt_brl(opcao4['VALOR_CONSOLIDADO'].sum()):>18}     │
└─────────────────────────────────────────────────────────────────────────────────────────┘
    """)
    
    print("""
📌 RECOMENDAÇÃO:

   A OPÇÃO 4 parece mais precisa para o estudo:
   - Pega apenas "ATIVA AJUIZADA" (exclui "a ser ajuizada", "em cobrança", etc.)
   - Exclui explicitamente quem já tem garantia
   - É o público que PRECISA de garantia agora
   
   A OPÇÃO 1 (TIPO_SITUACAO = 'Em cobrança') é mais ampla e inclui casos
   que ainda não foram ajuizados.
    """)
    
    # =========================================================================
    # ANÁLISE 7: Detalhamento da Opção 4 (recomendada)
    # =========================================================================
    print("\n" + "=" * 100)
    print("📊 7. DETALHAMENTO DA OPÇÃO 4 (RECOMENDADA)")
    print("=" * 100)
    
    det4 = opcao4.groupby('SITUACAO_INSCRICAO').agg({
        'NUMERO_INSCRICAO': 'count',
        'VALOR_CONSOLIDADO': 'sum'
    }).reset_index()
    det4 = det4.sort_values('INSCRICOES' if 'INSCRICOES' in det4.columns else 'NUMERO_INSCRICAO', ascending=False)
    det4.columns = ['SITUACAO', 'INSCRICOES', 'DIVIDA']
    
    print(f"\n{'SITUACAO':<75} {'INSCR':>10} {'DÍVIDA':>15}")
    print("-" * 105)
    
    for _, row in det4.iterrows():
        print(f"{row['SITUACAO'][:75]:<75} {row['INSCRICOES']:>10,} {fmt_brl(row['DIVIDA']):>15}")
    
    print("\n" + "=" * 100)
    print("🏁 ANÁLISE CONCLUÍDA!")
    print("=" * 100)


if __name__ == "__main__":
    main()
