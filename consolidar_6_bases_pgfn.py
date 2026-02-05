import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

# ================= CONFIG =================
os.chdir(r"C:\Rodrigo\BasePGFN\2025")

# Lista das 6 bases
BASES = [
    "arquivo_lai_SIDA_1_202512.csv",
    "arquivo_lai_SIDA_2_202512.csv",
    "arquivo_lai_SIDA_3_202512.csv",
    "arquivo_lai_SIDA_4_202512.csv",
    "arquivo_lai_SIDA_5_202512.csv",
    "arquivo_lai_SIDA_6_202512.csv",
]

# Tributos estruturais de empresas médio/grande porte
TRIBUTOS_ALVO = [
    "COFINS", "PIS", "IRPJ", "CSLL", "IPI", "IOF", "Imposto de Importação"
]

EXCLUIR_TERMOS = ["SIMPLES", "MEI", "IRRF", "MULTA", "CUSTAS"]

COLUNAS = [
    "CPF_CNPJ", "TIPO_PESSOA", "TIPO_DEVEDOR", "NOME_DEVEDOR", "UF_DEVEDOR",
    "UNIDADE_RESPONSAVEL", "NUMERO_INSCRICAO", "TIPO_SITUACAO_INSCRICAO",
    "SITUACAO_INSCRICAO", "RECEITA_PRINCIPAL", "DATA_INSCRICAO",
    "INDICADOR_AJUIZADO", "VALOR_CONSOLIDADO"
]

def fmt_brl(valor):
    if pd.isna(valor):
        return "R$ -"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def extrair_garantia(s):
    s = str(s).upper()
    if "SEGURO GARANTIA" in s: return "SEGURO GARANTIA"
    if "PENHORA" in s: return "PENHORA"
    if "CARTA FIANCA" in s or "CARTA FIANÇA" in s: return "CARTA FIANÇA"
    if "DEPOSITO" in s or "DEPÓSITO" in s: return "DEPÓSITO"
    return "SEM GARANTIA"

# ================= PROCESSAR CADA BASE =================
print("=" * 100)
print("📥 PROCESSANDO AS 6 BASES PGFN - BRASIL COMPLETO")
print("=" * 100)

resultados = []

for i, arquivo in enumerate(BASES, 1):
    print(f"\n{'='*80}")
    print(f"📂 BASE {i}: {arquivo}")
    print(f"{'='*80}")
    
    try:
        df = pd.read_csv(arquivo, sep=";", encoding="latin1", usecols=COLUNAS, low_memory=False)
        total_bruto = len(df)
        print(f"   Total registros brutos: {total_bruto:,}")
        
        # Filtro PJ
        pj_mask = df["TIPO_PESSOA"].astype(str).str.contains("jur", case=False, na=False)
        df = df[pj_mask].copy()
        total_pj = len(df)
        print(f"   🏢 Filtro PJ: {total_pj:,}")
        
        # Filtro tributos
        mask_tributos = df["RECEITA_PRINCIPAL"].astype(str).str.contains("|".join(TRIBUTOS_ALVO), case=False, na=False)
        mask_excluir = df["RECEITA_PRINCIPAL"].astype(str).str.contains("|".join(EXCLUIR_TERMOS), case=False, na=False)
        df = df[mask_tributos & ~mask_excluir].copy()
        total_filtrado = len(df)
        print(f"   🎯 Filtro tributos: {total_filtrado:,}")
        
        # Conversão valor
        df["VALOR_CONSOLIDADO"] = (
            df["VALOR_CONSOLIDADO"].astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        df["VALOR_CONSOLIDADO"] = pd.to_numeric(df["VALOR_CONSOLIDADO"], errors="coerce")
        
        # Features
        df["GARANTIA_ATUAL"] = df["SITUACAO_INSCRICAO"].apply(extrair_garantia)
        df["AJUIZADO_BIN"] = df["INDICADOR_AJUIZADO"].astype(str).str.upper().isin(["SIM", "S", "1", "TRUE"])
        
        # Métricas
        cnpjs_total = df["CPF_CNPJ"].nunique()
        divida_total = df["VALOR_CONSOLIDADO"].sum()
        
        # Ajuizados
        ajuizados = df[df["AJUIZADO_BIN"] == True]
        ajuizados_qtd = len(ajuizados)
        ajuizados_cnpjs = ajuizados["CPF_CNPJ"].nunique()
        ajuizados_divida = ajuizados["VALOR_CONSOLIDADO"].sum()
        
        # Por garantia (ajuizados)
        sg_qtd = len(ajuizados[ajuizados["GARANTIA_ATUAL"] == "SEGURO GARANTIA"])
        sg_divida = ajuizados[ajuizados["GARANTIA_ATUAL"] == "SEGURO GARANTIA"]["VALOR_CONSOLIDADO"].sum()
        sg_cnpjs = ajuizados[ajuizados["GARANTIA_ATUAL"] == "SEGURO GARANTIA"]["CPF_CNPJ"].nunique()
        
        carta_qtd = len(ajuizados[ajuizados["GARANTIA_ATUAL"] == "CARTA FIANÇA"])
        carta_divida = ajuizados[ajuizados["GARANTIA_ATUAL"] == "CARTA FIANÇA"]["VALOR_CONSOLIDADO"].sum()
        carta_cnpjs = ajuizados[ajuizados["GARANTIA_ATUAL"] == "CARTA FIANÇA"]["CPF_CNPJ"].nunique()
        
        deposito_qtd = len(ajuizados[ajuizados["GARANTIA_ATUAL"] == "DEPÓSITO"])
        deposito_divida = ajuizados[ajuizados["GARANTIA_ATUAL"] == "DEPÓSITO"]["VALOR_CONSOLIDADO"].sum()
        deposito_cnpjs = ajuizados[ajuizados["GARANTIA_ATUAL"] == "DEPÓSITO"]["CPF_CNPJ"].nunique()
        
        penhora_qtd = len(ajuizados[ajuizados["GARANTIA_ATUAL"] == "PENHORA"])
        penhora_divida = ajuizados[ajuizados["GARANTIA_ATUAL"] == "PENHORA"]["VALOR_CONSOLIDADO"].sum()
        penhora_cnpjs = ajuizados[ajuizados["GARANTIA_ATUAL"] == "PENHORA"]["CPF_CNPJ"].nunique()
        
        sem_qtd = len(ajuizados[ajuizados["GARANTIA_ATUAL"] == "SEM GARANTIA"])
        sem_divida = ajuizados[ajuizados["GARANTIA_ATUAL"] == "SEM GARANTIA"]["VALOR_CONSOLIDADO"].sum()
        sem_cnpjs = ajuizados[ajuizados["GARANTIA_ATUAL"] == "SEM GARANTIA"]["CPF_CNPJ"].nunique()
        
        # Médias e medianas (ajuizados sem garantia)
        sem_garantia_df = ajuizados[ajuizados["GARANTIA_ATUAL"] == "SEM GARANTIA"]
        sem_media = sem_garantia_df["VALOR_CONSOLIDADO"].mean() if len(sem_garantia_df) > 0 else 0
        sem_mediana = sem_garantia_df["VALOR_CONSOLIDADO"].median() if len(sem_garantia_df) > 0 else 0
        
        resultado = {
            "BASE": f"SIDA_{i}",
            "TOTAL_BRUTO": total_bruto,
            "TOTAL_PJ": total_pj,
            "TOTAL_FILTRADO": total_filtrado,
            "CNPJS_TOTAL": cnpjs_total,
            "DIVIDA_TOTAL": divida_total,
            "AJUIZADOS_QTD": ajuizados_qtd,
            "AJUIZADOS_CNPJS": ajuizados_cnpjs,
            "AJUIZADOS_DIVIDA": ajuizados_divida,
            "SG_QTD": sg_qtd,
            "SG_CNPJS": sg_cnpjs,
            "SG_DIVIDA": sg_divida,
            "CARTA_QTD": carta_qtd,
            "CARTA_CNPJS": carta_cnpjs,
            "CARTA_DIVIDA": carta_divida,
            "DEPOSITO_QTD": deposito_qtd,
            "DEPOSITO_CNPJS": deposito_cnpjs,
            "DEPOSITO_DIVIDA": deposito_divida,
            "PENHORA_QTD": penhora_qtd,
            "PENHORA_CNPJS": penhora_cnpjs,
            "PENHORA_DIVIDA": penhora_divida,
            "SEM_QTD": sem_qtd,
            "SEM_CNPJS": sem_cnpjs,
            "SEM_DIVIDA": sem_divida,
            "SEM_MEDIA": sem_media,
            "SEM_MEDIANA": sem_mediana,
        }
        
        resultados.append(resultado)
        
        print(f"\n   📊 RESUMO BASE {i}:")
        print(f"      CNPJs únicos: {cnpjs_total:,}")
        print(f"      Dívida total: {fmt_brl(divida_total)}")
        print(f"      Ajuizados: {ajuizados_qtd:,} inscrições | {ajuizados_cnpjs:,} CNPJs")
        print(f"      → Seguro Garantia: {sg_qtd:,} ({sg_cnpjs:,} CNPJs) | {fmt_brl(sg_divida)}")
        print(f"      → Carta Fiança: {carta_qtd:,} ({carta_cnpjs:,} CNPJs) | {fmt_brl(carta_divida)}")
        print(f"      → Depósito: {deposito_qtd:,} ({deposito_cnpjs:,} CNPJs) | {fmt_brl(deposito_divida)}")
        print(f"      → Penhora: {penhora_qtd:,} ({penhora_cnpjs:,} CNPJs) | {fmt_brl(penhora_divida)}")
        print(f"      → SEM GARANTIA: {sem_qtd:,} ({sem_cnpjs:,} CNPJs) | {fmt_brl(sem_divida)}")
        
    except Exception as e:
        print(f"   ❌ ERRO ao processar: {e}")
        continue

# ================= CONSOLIDAR RESULTADOS =================
print("\n" + "=" * 100)
print("📊 CONSOLIDAÇÃO - BRASIL COMPLETO (6 BASES)")
print("=" * 100)

df_result = pd.DataFrame(resultados)

# Totais
print(f"\n{'BASE':<10} {'REGISTROS':>12} {'PJ':>12} {'FILTRADO':>12} {'CNPJs':>10} {'AJUIZ_QTD':>12} {'AJUIZ_CNPJ':>12} {'SEM_GAR_QTD':>14} {'SEM_GAR_CNPJ':>14}")
print("-" * 120)

for _, row in df_result.iterrows():
    print(f"{row['BASE']:<10} {row['TOTAL_BRUTO']:>12,} {row['TOTAL_PJ']:>12,} {row['TOTAL_FILTRADO']:>12,} {row['CNPJS_TOTAL']:>10,} {row['AJUIZADOS_QTD']:>12,} {row['AJUIZADOS_CNPJS']:>12,} {row['SEM_QTD']:>14,} {row['SEM_CNPJS']:>14,}")

print("-" * 120)
print(f"{'TOTAL':<10} {df_result['TOTAL_BRUTO'].sum():>12,} {df_result['TOTAL_PJ'].sum():>12,} {df_result['TOTAL_FILTRADO'].sum():>12,} {df_result['CNPJS_TOTAL'].sum():>10,} {df_result['AJUIZADOS_QTD'].sum():>12,} {df_result['AJUIZADOS_CNPJS'].sum():>12,} {df_result['SEM_QTD'].sum():>14,} {df_result['SEM_CNPJS'].sum():>14,}")

# Dívidas
print(f"\n\n{'BASE':<10} {'DÍVIDA TOTAL':>25} {'DÍVIDA AJUIZADOS':>25} {'DÍVIDA SEM GARANTIA':>25}")
print("-" * 90)

for _, row in df_result.iterrows():
    print(f"{row['BASE']:<10} {fmt_brl(row['DIVIDA_TOTAL']):>25} {fmt_brl(row['AJUIZADOS_DIVIDA']):>25} {fmt_brl(row['SEM_DIVIDA']):>25}")

print("-" * 90)
print(f"{'TOTAL':<10} {fmt_brl(df_result['DIVIDA_TOTAL'].sum()):>25} {fmt_brl(df_result['AJUIZADOS_DIVIDA'].sum()):>25} {fmt_brl(df_result['SEM_DIVIDA'].sum()):>25}")

# Por tipo de garantia
print(f"\n\n📊 AJUIZADOS POR TIPO DE GARANTIA (BRASIL):")
print("-" * 100)
print(f"{'BASE':<10} {'SEGURO GAR':>12} {'CARTA FIANÇA':>14} {'DEPÓSITO':>12} {'PENHORA':>12} {'SEM GARANTIA':>14}")
print("-" * 100)

for _, row in df_result.iterrows():
    print(f"{row['BASE']:<10} {row['SG_QTD']:>12,} {row['CARTA_QTD']:>14,} {row['DEPOSITO_QTD']:>12,} {row['PENHORA_QTD']:>12,} {row['SEM_QTD']:>14,}")

print("-" * 100)
print(f"{'TOTAL':<10} {df_result['SG_QTD'].sum():>12,} {df_result['CARTA_QTD'].sum():>14,} {df_result['DEPOSITO_QTD'].sum():>12,} {df_result['PENHORA_QTD'].sum():>12,} {df_result['SEM_QTD'].sum():>14,}")

# ================= GRÁFICOS =================
print("\n\n📊 GERANDO GRÁFICOS...")

fig, axes = plt.subplots(2, 3, figsize=(18, 12))

cores = ['#3498db', '#2ecc71', '#e74c3c', '#f39c12', '#9b59b6', '#1abc9c']

# 1. Inscrições por base
ax1 = axes[0, 0]
bars1 = ax1.bar(df_result["BASE"], df_result["TOTAL_FILTRADO"], color=cores)
ax1.set_title("Inscrições PJ + Tributos por Base", fontweight="bold")
ax1.set_ylabel("Quantidade")
ax1.tick_params(axis='x', rotation=45)
for bar, val in zip(bars1, df_result["TOTAL_FILTRADO"]):
    ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{val:,.0f}', 
             ha='center', va='bottom', fontsize=8)

# 2. Ajuizados por base
ax2 = axes[0, 1]
bars2 = ax2.bar(df_result["BASE"], df_result["AJUIZADOS_QTD"], color=cores)
ax2.set_title("Inscrições Ajuizadas por Base", fontweight="bold")
ax2.set_ylabel("Quantidade")
ax2.tick_params(axis='x', rotation=45)
for bar, val in zip(bars2, df_result["AJUIZADOS_QTD"]):
    ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{val:,.0f}', 
             ha='center', va='bottom', fontsize=8)

# 3. Sem garantia por base
ax3 = axes[0, 2]
bars3 = ax3.bar(df_result["BASE"], df_result["SEM_QTD"], color=cores)
ax3.set_title("Ajuizados SEM GARANTIA por Base", fontweight="bold")
ax3.set_ylabel("Quantidade")
ax3.tick_params(axis='x', rotation=45)
for bar, val in zip(bars3, df_result["SEM_QTD"]):
    ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{val:,.0f}', 
             ha='center', va='bottom', fontsize=8)

# 4. Dívida sem garantia por base (em trilhões)
ax4 = axes[1, 0]
divida_tri = df_result["SEM_DIVIDA"] / 1e12
bars4 = ax4.bar(df_result["BASE"], divida_tri, color=cores)
ax4.set_title("Dívida Ajuizada SEM GARANTIA (R$ tri)", fontweight="bold")
ax4.set_ylabel("R$ trilhões")
ax4.tick_params(axis='x', rotation=45)
for bar, val in zip(bars4, divida_tri):
    ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{val:.1f}', 
             ha='center', va='bottom', fontsize=9)

# 5. Tipo de garantia (total Brasil) - Pizza
ax5 = axes[1, 1]
garantias = ["Seguro Garantia", "Carta Fiança", "Depósito", "Penhora", "Sem Garantia"]
valores_gar = [
    df_result["SG_QTD"].sum(),
    df_result["CARTA_QTD"].sum(),
    df_result["DEPOSITO_QTD"].sum(),
    df_result["PENHORA_QTD"].sum(),
    df_result["SEM_QTD"].sum()
]
cores_pizza = ['#27ae60', '#3498db', '#f39c12', '#e74c3c', '#95a5a6']
explode = (0.05, 0.05, 0.05, 0.05, 0.1)
ax5.pie(valores_gar, labels=garantias, autopct='%1.1f%%', colors=cores_pizza, explode=explode, startangle=90)
ax5.set_title("Distribuição por Tipo de Garantia (Ajuizados Brasil)", fontweight="bold")

# 6. Comparativo CNPJs
ax6 = axes[1, 2]
x = np.arange(len(df_result))
width = 0.35
bars6a = ax6.bar(x - width/2, df_result["AJUIZADOS_CNPJS"], width, label="Ajuizados Total", color='#3498db')
bars6b = ax6.bar(x + width/2, df_result["SEM_CNPJS"], width, label="Sem Garantia", color='#e74c3c')
ax6.set_title("CNPJs: Ajuizados vs Sem Garantia", fontweight="bold")
ax6.set_ylabel("CNPJs únicos")
ax6.set_xticks(x)
ax6.set_xticklabels(df_result["BASE"], rotation=45)
ax6.legend()

plt.suptitle("ANÁLISE PGFN BRASIL — 6 BASES CONSOLIDADAS", fontsize=14, fontweight="bold", y=1.02)
plt.tight_layout()
plt.savefig("consolidado_6_bases_pgfn.png", dpi=150, bbox_inches="tight")
plt.close()

print("✅ Gráfico salvo: consolidado_6_bases_pgfn.png")

# ================= EXPORTAR CSV =================
df_result.to_csv("consolidado_6_bases_pgfn.csv", sep=";", index=False, encoding="utf-8-sig")
print("✅ CSV salvo: consolidado_6_bases_pgfn.csv")

# ================= RESUMO FINAL =================
print("\n" + "=" * 100)
print("🎯 RESUMO BRASIL COMPLETO — MERCADO SEGURO GARANTIA JUDICIAL")
print("=" * 100)

total_inscricoes = df_result["TOTAL_FILTRADO"].sum()
total_cnpjs = df_result["CNPJS_TOTAL"].sum()
total_divida = df_result["DIVIDA_TOTAL"].sum()
total_ajuizados = df_result["AJUIZADOS_QTD"].sum()
total_ajuizados_cnpjs = df_result["AJUIZADOS_CNPJS"].sum()
total_sem = df_result["SEM_QTD"].sum()
total_sem_cnpjs = df_result["SEM_CNPJS"].sum()
total_sem_divida = df_result["SEM_DIVIDA"].sum()

print(f"\n📌 UNIVERSO TOTAL (PJ + Tributos estruturais):")
print(f"   Inscrições: {total_inscricoes:,}")
print(f"   CNPJs: {total_cnpjs:,}")
print(f"   Dívida: {fmt_brl(total_divida)}")

print(f"\n📌 AJUIZADOS:")
print(f"   Inscrições: {total_ajuizados:,}")
print(f"   CNPJs: {total_ajuizados_cnpjs:,}")
print(f"   Dívida: {fmt_brl(df_result['AJUIZADOS_DIVIDA'].sum())}")

print(f"\n📌 MERCADO ALVO (AJUIZADO + SEM GARANTIA):")
print(f"   Inscrições: {total_sem:,}")
print(f"   CNPJs: {total_sem_cnpjs:,}")
print(f"   Dívida: {fmt_brl(total_sem_divida)}")

print(f"\n📌 PENETRAÇÃO ATUAL:")
print(f"   Seguro Garantia: {df_result['SG_QTD'].sum():,} inscrições ({df_result['SG_CNPJS'].sum():,} CNPJs)")
print(f"   Carta Fiança: {df_result['CARTA_QTD'].sum():,} inscrições ({df_result['CARTA_CNPJS'].sum():,} CNPJs)")
print(f"   Depósito: {df_result['DEPOSITO_QTD'].sum():,} inscrições ({df_result['DEPOSITO_CNPJS'].sum():,} CNPJs)")
print(f"   Penhora: {df_result['PENHORA_QTD'].sum():,} inscrições ({df_result['PENHORA_CNPJS'].sum():,} CNPJs)")

pct_sem = (total_sem / total_ajuizados * 100) if total_ajuizados > 0 else 0
print(f"\n📌 GAP DE MERCADO:")
print(f"   {pct_sem:.2f}% dos ajuizados estão SEM GARANTIA")

premio_potencial = total_sem_divida * 0.02
comissao_potencial = premio_potencial * 0.25

print(f"\n📌 POTENCIAL COMERCIAL (estimativa):")
print(f"   Prêmio total (2%): {fmt_brl(premio_potencial)}")
print(f"   Comissão (25%): {fmt_brl(comissao_potencial)}")

print("\n" + "=" * 100)
print("🏁 ANÁLISE CONCLUÍDA!")
print("=" * 100)
