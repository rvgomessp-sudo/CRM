import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
import os

# ================= CONFIG =================
os.chdir(r"C:\Rodrigo\BasePGFN\2025")

BASES = [
    ("arquivo_lai_SIDA_1_202512.csv", "1ª Região"),
    ("arquivo_lai_SIDA_2_202512.csv", "2ª Região"),
    ("arquivo_lai_SIDA_3_202512.csv", "3ª Região"),
    ("arquivo_lai_SIDA_4_202512.csv", "4ª Região"),
    ("arquivo_lai_SIDA_5_202512.csv", "5ª Região"),
    ("arquivo_lai_SIDA_6_202512.csv", "6ª Região"),
]

TRIBUTOS_ALVO = ["COFINS", "PIS", "IRPJ", "CSLL", "IPI", "IOF", "Imposto de Importação"]
EXCLUIR_TERMOS = ["SIMPLES", "MEI", "IRRF", "MULTA", "CUSTAS"]

COLUNAS = [
    "CPF_CNPJ", "TIPO_PESSOA", "NOME_DEVEDOR", "UF_DEVEDOR",
    "NUMERO_INSCRICAO", "SITUACAO_INSCRICAO", "RECEITA_PRINCIPAL", 
    "DATA_INSCRICAO", "INDICADOR_AJUIZADO", "VALOR_CONSOLIDADO"
]

# ================= CORTE TEMPORAL =================
SAFRAS_VALIDAS = ["2024-2025", "2023", "2022", "2021", "2020"]  # Últimos 5 anos
IDADE_MAXIMA = 6  # Anos (até 2020)

def fmt_brl(valor):
    if pd.isna(valor): return "R$ -"
    if valor >= 1e12: return f"R$ {valor/1e12:.1f} tri"
    if valor >= 1e9: return f"R$ {valor/1e9:.1f} bi"
    if valor >= 1e6: return f"R$ {valor/1e6:.1f} mi"
    return f"R$ {valor:,.0f}"

def extrair_garantia(s):
    s = str(s).upper()
    if "SEGURO GARANTIA" in s: return "SEGURO GARANTIA"
    if "PENHORA" in s: return "PENHORA"
    if "CARTA FIANCA" in s or "CARTA FIANÇA" in s: return "CARTA FIANÇA"
    if "DEPOSITO" in s or "DEPÓSITO" in s: return "DEPÓSITO"
    return "SEM GARANTIA"

def classificar_safra(idade):
    if pd.isna(idade): return "INVALIDO"
    if idade <= 1: return "2024-2025"
    if idade <= 2: return "2023"
    if idade <= 3: return "2022"
    if idade <= 4: return "2021"
    if idade <= 5: return "2020"
    if idade <= 6: return "2019"
    return "≤2018"

# ================= PROCESSAR BASES =================
print("=" * 100)
print("📊 ANÁLISE COM CORTE TEMPORAL: 2020-2025 (ÚLTIMOS 5 ANOS)")
print("=" * 100)

df_all = []

for arquivo, nome_regiao in BASES:
    print(f"\n📂 Processando {nome_regiao}...")
    
    df = pd.read_csv(arquivo, sep=";", encoding="latin1", usecols=COLUNAS, low_memory=False)
    
    # Filtros básicos
    pj_mask = df["TIPO_PESSOA"].astype(str).str.contains("jur", case=False, na=False)
    df = df[pj_mask].copy()
    
    mask_tributos = df["RECEITA_PRINCIPAL"].astype(str).str.contains("|".join(TRIBUTOS_ALVO), case=False, na=False)
    mask_excluir = df["RECEITA_PRINCIPAL"].astype(str).str.contains("|".join(EXCLUIR_TERMOS), case=False, na=False)
    df = df[mask_tributos & ~mask_excluir].copy()
    
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
    
    # Data e idade
    df["DATA_INSCRICAO"] = pd.to_datetime(df["DATA_INSCRICAO"], errors="coerce", dayfirst=True)
    hoje = pd.Timestamp.today().normalize()
    df["IDADE_ANOS"] = (hoje - df["DATA_INSCRICAO"]).dt.days / 365.25
    df["SAFRA"] = df["IDADE_ANOS"].apply(classificar_safra)
    
    # Só ajuizados sem garantia
    df = df[(df["AJUIZADO_BIN"] == True) & (df["GARANTIA_ATUAL"] == "SEM GARANTIA")].copy()
    df["REGIAO"] = nome_regiao
    
    df_all.append(df)
    print(f"   Total (sem filtro safra): {len(df):,} inscrições | {df['CPF_CNPJ'].nunique():,} CNPJs")

# Consolidar
df_brasil_completo = pd.concat(df_all, ignore_index=True)
print(f"\n📊 Total Brasil (ANTES do corte): {len(df_brasil_completo):,} inscrições | {df_brasil_completo['CPF_CNPJ'].nunique():,} CNPJs")

# ================= APLICAR CORTE TEMPORAL =================
print("\n" + "=" * 100)
print("✂️ APLICANDO CORTE: SAFRAS 2020-2025")
print("=" * 100)

df_brasil = df_brasil_completo[df_brasil_completo["SAFRA"].isin(SAFRAS_VALIDAS)].copy()

print(f"\n📊 Total Brasil (APÓS corte 2020-2025):")
print(f"   Inscrições: {len(df_brasil):,} (de {len(df_brasil_completo):,} = {len(df_brasil)/len(df_brasil_completo)*100:.1f}%)")
print(f"   CNPJs: {df_brasil['CPF_CNPJ'].nunique():,} (de {df_brasil_completo['CPF_CNPJ'].nunique():,})")
print(f"   Dívida: {fmt_brl(df_brasil['VALOR_CONSOLIDADO'].sum())} (de {fmt_brl(df_brasil_completo['VALOR_CONSOLIDADO'].sum())})")

# Removidos
removidos = len(df_brasil_completo) - len(df_brasil)
removidos_divida = df_brasil_completo['VALOR_CONSOLIDADO'].sum() - df_brasil['VALOR_CONSOLIDADO'].sum()
print(f"\n📌 REMOVIDOS (≤2019):")
print(f"   Inscrições: {removidos:,} ({removidos/len(df_brasil_completo)*100:.1f}%)")
print(f"   Dívida: {fmt_brl(removidos_divida)} ({removidos_divida/df_brasil_completo['VALOR_CONSOLIDADO'].sum()*100:.1f}%)")

# ================= ANÁLISE POR SAFRA (2020-2025) =================
print("\n" + "=" * 100)
print("📊 DISTRIBUIÇÃO POR SAFRA (2020-2025)")
print("=" * 100)

safra_stats = df_brasil.groupby("SAFRA").agg(
    INSCRICOES=("NUMERO_INSCRICAO", "count"),
    CNPJs=("CPF_CNPJ", "nunique"),
    DIVIDA=("VALOR_CONSOLIDADO", "sum"),
    MEDIA=("VALOR_CONSOLIDADO", "mean"),
    MEDIANA=("VALOR_CONSOLIDADO", "median"),
    P25=("VALOR_CONSOLIDADO", lambda x: x.quantile(0.25)),
    P75=("VALOR_CONSOLIDADO", lambda x: x.quantile(0.75)),
).reindex(SAFRAS_VALIDAS)

print(f"\n{'SAFRA':<12} {'INSCR':>10} {'CNPJs':>10} {'DÍVIDA':>15} {'MÉDIA':>12} {'MEDIANA':>12} {'P25':>12} {'P75':>12}")
print("-" * 105)
for safra in SAFRAS_VALIDAS:
    if safra in safra_stats.index:
        row = safra_stats.loc[safra]
        print(f"{safra:<12} {int(row['INSCRICOES']):>10,} {int(row['CNPJs']):>10,} {fmt_brl(row['DIVIDA']):>15} {fmt_brl(row['MEDIA']):>12} {fmt_brl(row['MEDIANA']):>12} {fmt_brl(row['P25']):>12} {fmt_brl(row['P75']):>12}")

print("-" * 105)
print(f"{'TOTAL':<12} {len(df_brasil):>10,} {df_brasil['CPF_CNPJ'].nunique():>10,} {fmt_brl(df_brasil['VALOR_CONSOLIDADO'].sum()):>15}")

# ================= ANÁLISE POR REGIÃO (2020-2025) =================
print("\n" + "=" * 100)
print("📊 DISTRIBUIÇÃO POR REGIÃO (2020-2025)")
print("=" * 100)

regiao_stats = df_brasil.groupby("REGIAO").agg(
    INSCRICOES=("NUMERO_INSCRICAO", "count"),
    CNPJs=("CPF_CNPJ", "nunique"),
    DIVIDA=("VALOR_CONSOLIDADO", "sum"),
    MEDIA=("VALOR_CONSOLIDADO", "mean"),
    MEDIANA=("VALOR_CONSOLIDADO", "median"),
)

print(f"\n{'REGIÃO':<20} {'INSCR':>10} {'CNPJs':>10} {'DÍVIDA':>15} {'MÉDIA':>12} {'MEDIANA':>12}")
print("-" * 85)
for regiao in regiao_stats.index:
    row = regiao_stats.loc[regiao]
    print(f"{regiao:<20} {int(row['INSCRICOES']):>10,} {int(row['CNPJs']):>10,} {fmt_brl(row['DIVIDA']):>15} {fmt_brl(row['MEDIA']):>12} {fmt_brl(row['MEDIANA']):>12}")

# ================= ANÁLISE DE TICKET (2020-2025) =================
print("\n" + "=" * 100)
print("📊 ANÁLISE DE TICKET (2020-2025)")
print("=" * 100)

faixas = [
    ("< R$ 500 mil", 0, 500_000),
    ("R$ 500k - R$ 1 mi", 500_000, 1_000_000),
    ("R$ 1 mi - R$ 2 mi", 1_000_000, 2_000_000),
    ("R$ 2 mi - R$ 5 mi", 2_000_000, 5_000_000),
    ("R$ 5 mi - R$ 10 mi", 5_000_000, 10_000_000),
    ("R$ 10 mi - R$ 50 mi", 10_000_000, 50_000_000),
    ("R$ 50 mi - R$ 100 mi", 50_000_000, 100_000_000),
    ("R$ 100 mi - R$ 500 mi", 100_000_000, 500_000_000),
    ("> R$ 500 mi", 500_000_000, float("inf")),
]

print(f"\n{'FAIXA':<25} {'INSCRIÇÕES':>12} {'%':>8} {'CNPJs':>10} {'DÍVIDA':>18} {'% DÍVIDA':>10}")
print("-" * 90)

for nome, vmin, vmax in faixas:
    subset = df_brasil[(df_brasil["VALOR_CONSOLIDADO"] >= vmin) & (df_brasil["VALOR_CONSOLIDADO"] < vmax)]
    pct_inscr = len(subset) / len(df_brasil) * 100
    pct_divida = subset["VALOR_CONSOLIDADO"].sum() / df_brasil["VALOR_CONSOLIDADO"].sum() * 100
    print(f"{nome:<25} {len(subset):>12,} {pct_inscr:>7.2f}% {subset['CPF_CNPJ'].nunique():>10,} {fmt_brl(subset['VALOR_CONSOLIDADO'].sum()):>18} {pct_divida:>9.2f}%")

# ================= LÓGICA DE EXCLUSÃO POR TICKET (2020-2025) =================
print("\n" + "=" * 100)
print("📊 LÓGICA DE EXCLUSÃO POR TICKET (R$ 1 MI)")
print("=" * 100)

TICKET_MIN = 1_000_000

# Agregar por CNPJ
cnpj_agg = df_brasil.groupby("CPF_CNPJ").agg(
    NOME=("NOME_DEVEDOR", "first"),
    UF=("UF_DEVEDOR", "first"),
    TOTAL_INSCRICOES=("NUMERO_INSCRICAO", "count"),
    DIVIDA_TOTAL=("VALOR_CONSOLIDADO", "sum"),
    INSCR_ACIMA_1MI=("VALOR_CONSOLIDADO", lambda x: (x >= TICKET_MIN).sum()),
    DIVIDA_ACIMA_1MI=("VALOR_CONSOLIDADO", lambda x: x[x >= TICKET_MIN].sum()),
    INSCR_ABAIXO_1MI=("VALOR_CONSOLIDADO", lambda x: (x < TICKET_MIN).sum()),
    DIVIDA_ABAIXO_1MI=("VALOR_CONSOLIDADO", lambda x: x[x < TICKET_MIN].sum()),
    SAFRAS=("SAFRA", lambda x: ", ".join(sorted(x.unique()))),
).reset_index()

# Classificar CNPJs
cnpj_agg["CLASSIFICACAO"] = cnpj_agg.apply(
    lambda row: "EXCLUIR" if row["INSCR_ACIMA_1MI"] == 0 
    else "MANTER_INTEGRAL" if row["INSCR_ABAIXO_1MI"] == 0
    else "MANTER_FILTRAR", axis=1
)

print("\n📌 CLASSIFICAÇÃO DE CNPJs:")
for classif in ["MANTER_INTEGRAL", "MANTER_FILTRAR", "EXCLUIR"]:
    subset = cnpj_agg[cnpj_agg["CLASSIFICACAO"] == classif]
    print(f"\n   {classif}:")
    print(f"      CNPJs: {len(subset):,}")
    print(f"      Inscrições: {subset['TOTAL_INSCRICOES'].sum():,}")
    print(f"      Dívida total: {fmt_brl(subset['DIVIDA_TOTAL'].sum())}")
    print(f"      Dívida ≥ R$ 1mi: {fmt_brl(subset['DIVIDA_ACIMA_1MI'].sum())}")

# Base final
cnpjs_manter = cnpj_agg[cnpj_agg["CLASSIFICACAO"] != "EXCLUIR"]
print(f"\n📌 BASE FINAL (CNPJs com pelo menos 1 inscrição ≥ R$ 1mi):")
print(f"   CNPJs: {len(cnpjs_manter):,}")
print(f"   Dívida relevante (≥ R$ 1mi): {fmt_brl(cnpjs_manter['DIVIDA_ACIMA_1MI'].sum())}")

# Potencial comercial
premio = cnpjs_manter['DIVIDA_ACIMA_1MI'].sum() * 0.02
comissao = premio * 0.25
print(f"\n📌 POTENCIAL COMERCIAL (base limpa 2020-2025, ≥ R$ 1mi):")
print(f"   Prêmio estimado (2%): {fmt_brl(premio)}")
print(f"   Comissão estimada (25%): {fmt_brl(comissao)}")

# ================= GRÁFICOS =================
print("\n📊 Gerando gráficos...")

fig, axes = plt.subplots(2, 3, figsize=(18, 12))

cores_safras = {'2024-2025': '#3498db', '2023': '#2ecc71', '2022': '#f1c40f', 
                '2021': '#e67e22', '2020': '#e74c3c'}

# 1. Inscrições por safra
ax1 = axes[0, 0]
safra_inscr = safra_stats["INSCRICOES"]
bars1 = ax1.bar(safra_inscr.index, safra_inscr.values, color=[cores_safras[s] for s in safra_inscr.index])
ax1.set_title("Inscrições por Safra (2020-2025)", fontweight='bold')
ax1.set_ylabel("Quantidade")
for bar, val in zip(bars1, safra_inscr.values):
    ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{val:,.0f}', ha='center', va='bottom', fontsize=9)

# 2. Dívida por safra
ax2 = axes[0, 1]
safra_divida = safra_stats["DIVIDA"] / 1e12
bars2 = ax2.bar(safra_divida.index, safra_divida.values, color=[cores_safras[s] for s in safra_divida.index])
ax2.set_title("Dívida por Safra (R$ trilhões)", fontweight='bold')
ax2.set_ylabel("R$ trilhões")
for bar, val in zip(bars2, safra_divida.values):
    ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{val:.1f}', ha='center', va='bottom', fontsize=9)

# 3. Média e Mediana por safra
ax3 = axes[0, 2]
x = np.arange(len(SAFRAS_VALIDAS))
width = 0.35
bars3a = ax3.bar(x - width/2, safra_stats["MEDIA"]/1e6, width, label='Média', color='#3498db')
bars3b = ax3.bar(x + width/2, safra_stats["MEDIANA"]/1e6, width, label='Mediana', color='#e74c3c')
ax3.set_title("Média vs Mediana por Safra (R$ mi)", fontweight='bold')
ax3.set_ylabel("R$ milhões")
ax3.set_xticks(x)
ax3.set_xticklabels(SAFRAS_VALIDAS)
ax3.legend()

# 4. Distribuição de valores (histograma)
ax4 = axes[1, 0]
df_brasil["VALOR_LOG"] = np.log10(df_brasil["VALOR_CONSOLIDADO"].clip(lower=1))
ax4.hist(df_brasil["VALOR_LOG"], bins=50, color='#3498db', alpha=0.7, edgecolor='white')
ax4.axvline(np.log10(1_000_000), color='red', linestyle='--', linewidth=2, label='R$ 1 mi')
ax4.axvline(np.log10(5_000_000), color='orange', linestyle='--', linewidth=2, label='R$ 5 mi')
ax4.set_title("Distribuição de Valores (log10) — 2020-2025", fontweight='bold')
ax4.set_xlabel("log10(Valor)")
ax4.set_ylabel("Frequência")
ax4.legend()

# 5. CNPJs por região
ax5 = axes[1, 1]
regiao_cnpjs = regiao_stats["CNPJs"].sort_values(ascending=True)
bars5 = ax5.barh(regiao_cnpjs.index, regiao_cnpjs.values, color='#27ae60')
ax5.set_title("CNPJs por Região (2020-2025)", fontweight='bold')
ax5.set_xlabel("CNPJs")
for bar, val in zip(bars5, regiao_cnpjs.values):
    ax5.text(bar.get_width() + 500, bar.get_y() + bar.get_height()/2, f'{val:,}', va='center', fontsize=9)

# 6. Pizza classificação
ax6 = axes[1, 2]
class_counts = cnpj_agg["CLASSIFICACAO"].value_counts()
colors_class = {'MANTER_INTEGRAL': '#27ae60', 'MANTER_FILTRAR': '#f39c12', 'EXCLUIR': '#e74c3c'}
ax6.pie(class_counts.values, labels=class_counts.index, autopct='%1.1f%%', 
        colors=[colors_class[c] for c in class_counts.index], startangle=90)
ax6.set_title("Classificação de CNPJs\n(Regra Ticket R$ 1 mi)", fontweight='bold')

plt.suptitle("ANÁLISE BASE LIMPA — SAFRAS 2020-2025", fontsize=14, fontweight='bold', y=1.02)
plt.tight_layout()
plt.savefig("analise_2020_2025_base_limpa.png", dpi=150, bbox_inches='tight')
plt.close()
print("✅ Gráfico salvo: analise_2020_2025_base_limpa.png")

# ================= EXPORTAR =================
cnpj_agg.to_csv("cnpjs_2020_2025_classificados.csv", sep=";", index=False, encoding="utf-8-sig")
print("✅ CSV salvo: cnpjs_2020_2025_classificados.csv")

# Base final (CNPJs a manter)
cnpjs_manter.to_csv("cnpjs_2020_2025_base_final.csv", sep=";", index=False, encoding="utf-8-sig")
print(f"✅ CSV salvo: cnpjs_2020_2025_base_final.csv ({len(cnpjs_manter):,} CNPJs)")

# Inscrições da base final (≥ R$ 1mi)
df_final = df_brasil[
    (df_brasil["CPF_CNPJ"].isin(cnpjs_manter["CPF_CNPJ"])) & 
    (df_brasil["VALOR_CONSOLIDADO"] >= TICKET_MIN)
].copy()
df_final.to_csv("inscricoes_2020_2025_base_final.csv", sep=";", index=False, encoding="utf-8-sig")
print(f"✅ CSV salvo: inscricoes_2020_2025_base_final.csv ({len(df_final):,} inscrições)")

print("\n" + "=" * 100)
print("🏁 ANÁLISE 2020-2025 CONCLUÍDA!")
print("=" * 100)

# ================= RESUMO FINAL =================
print("\n" + "=" * 100)
print("📋 RESUMO EXECUTIVO — BASE LIMPA 2020-2025")
print("=" * 100)

print(f"""
┌─────────────────────────────────────────────────────────────────┐
│  PREMISSAS APLICADAS                                            │
├─────────────────────────────────────────────────────────────────┤
│  ✓ Pessoa Jurídica                                              │
│  ✓ Tributos estruturais (IRPJ, CSLL, COFINS, PIS, IPI, IOF, II)│
│  ✓ Ajuizado = SIM                                               │
│  ✓ Sem garantia atual                                           │
│  ✓ Safra 2020-2025 (últimos 5 anos)                            │
│  ✓ Ticket mínimo R$ 1 milhão                                    │
├─────────────────────────────────────────────────────────────────┤
│  RESULTADO                                                       │
├─────────────────────────────────────────────────────────────────┤
│  CNPJs qualificados: {len(cnpjs_manter):>10,}                              │
│  Inscrições relevantes: {len(df_final):>10,}                           │
│  Dívida endereçável: {fmt_brl(cnpjs_manter['DIVIDA_ACIMA_1MI'].sum()):>15}                      │
│  Prêmio potencial (2%): {fmt_brl(premio):>15}                      │
│  Comissão potencial (25%): {fmt_brl(comissao):>12}                      │
└─────────────────────────────────────────────────────────────────┘
""")
