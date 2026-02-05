import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
import os

# ================= CONFIG =================
os.chdir(r"C:\Rodrigo\BasePGFN\2025")

BASES = [
    ("arquivo_lai_SIDA_1_202512.csv", "1ª Região (DF/GO/MT/TO/Norte)"),
    ("arquivo_lai_SIDA_2_202512.csv", "2ª Região (RJ/ES)"),
    ("arquivo_lai_SIDA_3_202512.csv", "3ª Região (SP/MS)"),
    ("arquivo_lai_SIDA_4_202512.csv", "4ª Região (RS/SC/PR)"),
    ("arquivo_lai_SIDA_5_202512.csv", "5ª Região (PE/AL/PB/RN/CE/SE)"),
    ("arquivo_lai_SIDA_6_202512.csv", "6ª Região (MG)"),
]

TRIBUTOS_ALVO = ["COFINS", "PIS", "IRPJ", "CSLL", "IPI", "IOF", "Imposto de Importação"]
EXCLUIR_TERMOS = ["SIMPLES", "MEI", "IRRF", "MULTA", "CUSTAS"]

COLUNAS = [
    "CPF_CNPJ", "TIPO_PESSOA", "NOME_DEVEDOR", "UF_DEVEDOR",
    "NUMERO_INSCRICAO", "SITUACAO_INSCRICAO", "RECEITA_PRINCIPAL", 
    "DATA_INSCRICAO", "INDICADOR_AJUIZADO", "VALOR_CONSOLIDADO"
]

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
    if pd.isna(idade): return "SEM DATA"
    if idade <= 1: return "2024-2025"
    if idade <= 2: return "2023"
    if idade <= 3: return "2022"
    if idade <= 4: return "2021"
    if idade <= 5: return "2020"
    if idade <= 6: return "2019"
    return "≤2018"

# ================= PROCESSAR BASES =================
print("=" * 100)
print("📊 ANÁLISE DE DISTRIBUIÇÃO TEMPORAL POR REGIÃO")
print("=" * 100)

resultados_regiao = []
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
    print(f"   ✅ {len(df):,} inscrições | {df['CPF_CNPJ'].nunique():,} CNPJs")

# Consolidar
df_brasil = pd.concat(df_all, ignore_index=True)
print(f"\n📊 Total Brasil: {len(df_brasil):,} inscrições | {df_brasil['CPF_CNPJ'].nunique():,} CNPJs")

# ================= HIPÓTESE 1: DISTRIBUIÇÃO TEMPORAL POR REGIÃO =================
print("\n" + "=" * 100)
print("📊 HIPÓTESE 1: DISTRIBUIÇÃO TEMPORAL É CONGRUENTE ENTRE REGIÕES?")
print("=" * 100)

# Tabela cruzada: Região x Safra (percentual)
ordem_safras_full = ["2024-2025", "2023", "2022", "2021", "2020", "2019", "≤2018", "SEM DATA"]

# Contagem absoluta
cross_abs_raw = pd.crosstab(df_brasil["REGIAO"], df_brasil["SAFRA"])
# Filtrar apenas colunas que existem
ordem_safras = [s for s in ordem_safras_full if s in cross_abs_raw.columns]
cross_abs = cross_abs_raw[ordem_safras]
print("\n📌 CONTAGEM ABSOLUTA (Inscrições por Região x Safra):")
print(cross_abs.to_string())

# Percentual por linha (cada região = 100%)
cross_pct_raw = pd.crosstab(df_brasil["REGIAO"], df_brasil["SAFRA"], normalize='index') * 100
cross_pct = cross_pct_raw[[s for s in ordem_safras if s in cross_pct_raw.columns]]
print("\n📌 PERCENTUAL POR REGIÃO (cada região = 100%):")
print(cross_pct.round(2).to_string())

# Teste Chi-quadrado
chi2, p_value, dof, expected = stats.chi2_contingency(cross_abs)
print(f"\n📌 TESTE CHI-QUADRADO:")
print(f"   Chi² = {chi2:,.2f}")
print(f"   p-valor = {p_value:.2e}")
print(f"   Graus de liberdade = {dof}")
print(f"   → {'DISTRIBUIÇÕES DIFERENTES' if p_value < 0.05 else 'DISTRIBUIÇÕES SIMILARES'} (α=0.05)")

# Cramér's V (tamanho do efeito)
n = cross_abs.sum().sum()
min_dim = min(cross_abs.shape) - 1
cramers_v = np.sqrt(chi2 / (n * min_dim))
print(f"   Cramér's V = {cramers_v:.4f} ({'fraco' if cramers_v < 0.1 else 'moderado' if cramers_v < 0.3 else 'forte'})")

# ================= HIPÓTESE 2: IMPACTO DO CORTE DE TICKET =================
print("\n" + "=" * 100)
print("📊 HIPÓTESE 2: IMPACTO DO CORTE DE TICKET (R$ 1 MI)")
print("=" * 100)

# Cenários de corte
cortes = [500_000, 1_000_000, 2_000_000, 5_000_000]

print("\n📌 ANÁLISE POR FAIXA DE VALOR (Brasil - Ajuizado Sem Garantia):")
print(f"\n{'FAIXA':<25} {'INSCRIÇÕES':>12} {'%':>8} {'CNPJs':>10} {'DÍVIDA':>18}")
print("-" * 80)

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

for nome, vmin, vmax in faixas:
    subset = df_brasil[(df_brasil["VALOR_CONSOLIDADO"] >= vmin) & (df_brasil["VALOR_CONSOLIDADO"] < vmax)]
    pct = len(subset) / len(df_brasil) * 100
    print(f"{nome:<25} {len(subset):>12,} {pct:>7.2f}% {subset['CPF_CNPJ'].nunique():>10,} {fmt_brl(subset['VALOR_CONSOLIDADO'].sum()):>18}")

# ================= HIPÓTESE 3: LÓGICA DE EXCLUSÃO DE CNPJ =================
print("\n" + "=" * 100)
print("📊 HIPÓTESE 3: LÓGICA DE EXCLUSÃO POR TICKET")
print("=" * 100)

TICKET_MIN = 1_000_000  # R$ 1 milhão

# Agregar por CNPJ
print("\n📌 Agregando por CNPJ...")
cnpj_agg = df_brasil.groupby("CPF_CNPJ").agg(
    TOTAL_INSCRICOES=("NUMERO_INSCRICAO", "count"),
    DIVIDA_TOTAL=("VALOR_CONSOLIDADO", "sum"),
    INSCR_ACIMA_1MI=("VALOR_CONSOLIDADO", lambda x: (x >= TICKET_MIN).sum()),
    DIVIDA_ACIMA_1MI=("VALOR_CONSOLIDADO", lambda x: x[x >= TICKET_MIN].sum()),
    INSCR_ABAIXO_1MI=("VALOR_CONSOLIDADO", lambda x: (x < TICKET_MIN).sum()),
    DIVIDA_ABAIXO_1MI=("VALOR_CONSOLIDADO", lambda x: x[x < TICKET_MIN].sum()),
).reset_index()

# Classificar CNPJs
cnpj_agg["CLASSIFICACAO"] = cnpj_agg.apply(
    lambda row: "EXCLUIR (só < R$ 1mi)" if row["INSCR_ACIMA_1MI"] == 0 
    else "MANTER (todas ≥ R$ 1mi)" if row["INSCR_ABAIXO_1MI"] == 0
    else "MANTER (mix - filtrar inscrições)", axis=1
)

print("\n📌 CLASSIFICAÇÃO DE CNPJs:")
class_resumo = cnpj_agg.groupby("CLASSIFICACAO").agg(
    CNPJs=("CPF_CNPJ", "count"),
    INSCR_TOTAL=("TOTAL_INSCRICOES", "sum"),
    DIVIDA_TOTAL=("DIVIDA_TOTAL", "sum"),
    DIVIDA_ACIMA_1MI=("DIVIDA_ACIMA_1MI", "sum"),
).reset_index()

for _, row in class_resumo.iterrows():
    print(f"\n   {row['CLASSIFICACAO']}:")
    print(f"      CNPJs: {row['CNPJs']:,}")
    print(f"      Inscrições: {row['INSCR_TOTAL']:,}")
    print(f"      Dívida total: {fmt_brl(row['DIVIDA_TOTAL'])}")
    print(f"      Dívida ≥ R$ 1mi: {fmt_brl(row['DIVIDA_ACIMA_1MI'])}")

# Calcular impacto
cnpjs_manter = cnpj_agg[cnpj_agg["CLASSIFICACAO"] != "EXCLUIR (só < R$ 1mi)"]
cnpjs_excluir = cnpj_agg[cnpj_agg["CLASSIFICACAO"] == "EXCLUIR (só < R$ 1mi)"]

print(f"\n📌 IMPACTO DA REGRA DE TICKET:")
print(f"   CNPJs ANTES: {len(cnpj_agg):,}")
print(f"   CNPJs DEPOIS: {len(cnpjs_manter):,} (-{len(cnpjs_excluir):,} = -{len(cnpjs_excluir)/len(cnpj_agg)*100:.1f}%)")
print(f"   Dívida relevante (≥ R$ 1mi): {fmt_brl(cnpjs_manter['DIVIDA_ACIMA_1MI'].sum())}")

# ================= HIPÓTESE 4: SAFRA x TICKET =================
print("\n" + "=" * 100)
print("📊 HIPÓTESE 4: INTERAÇÃO SAFRA x TICKET")
print("=" * 100)

# Filtrar inscrições ≥ R$ 1mi
df_filtrado = df_brasil[df_brasil["VALOR_CONSOLIDADO"] >= TICKET_MIN].copy()

print(f"\n📌 Após filtro ≥ R$ 1 mi:")
print(f"   Inscrições: {len(df_filtrado):,} (de {len(df_brasil):,} = {len(df_filtrado)/len(df_brasil)*100:.1f}%)")
print(f"   CNPJs: {df_filtrado['CPF_CNPJ'].nunique():,}")
print(f"   Dívida: {fmt_brl(df_filtrado['VALOR_CONSOLIDADO'].sum())}")

# Distribuição por safra (filtrado)
print("\n📌 DISTRIBUIÇÃO POR SAFRA (≥ R$ 1 mi):")
safra_filtrado = df_filtrado.groupby("SAFRA").agg(
    INSCRICOES=("NUMERO_INSCRICAO", "count"),
    CNPJs=("CPF_CNPJ", "nunique"),
    DIVIDA=("VALOR_CONSOLIDADO", "sum"),
    MEDIA=("VALOR_CONSOLIDADO", "mean"),
    MEDIANA=("VALOR_CONSOLIDADO", "median"),
).reindex([s for s in ordem_safras if s != "SEM DATA"])

print(f"\n{'SAFRA':<12} {'INSCR':>10} {'CNPJs':>10} {'DÍVIDA':>18} {'MÉDIA':>15} {'MEDIANA':>15}")
print("-" * 95)
for safra in [s for s in ordem_safras if s != "SEM DATA"]:
    if safra in safra_filtrado.index and not pd.isna(safra_filtrado.loc[safra, "INSCRICOES"]):
        row = safra_filtrado.loc[safra]
        print(f"{safra:<12} {int(row['INSCRICOES']):>10,} {int(row['CNPJs']):>10,} {fmt_brl(row['DIVIDA']):>18} {fmt_brl(row['MEDIA']):>15} {fmt_brl(row['MEDIANA']):>15}")

# ================= GRÁFICOS =================
print("\n📊 Gerando gráficos...")

fig, axes = plt.subplots(2, 3, figsize=(18, 12))

cores_safras = {'2024-2025': '#3498db', '2023': '#2ecc71', '2022': '#f1c40f', 
                '2021': '#e67e22', '2020': '#e74c3c', '2019': '#9b59b6', '≤2018': '#95a5a6', 'SEM DATA': '#bdc3c7'}

# 1. Distribuição % por região (stacked bar)
ax1 = axes[0, 0]
safras_plot = [s for s in ["2024-2025", "2023", "2022", "2021", "2020", "2019", "≤2018"] if s in cross_pct.columns]
cross_pct_plot = cross_pct[safras_plot]
cross_pct_plot.plot(kind='bar', stacked=True, ax=ax1, 
                     color=[cores_safras[s] for s in cross_pct_plot.columns])
ax1.set_title("Distribuição % por Safra (cada região = 100%)", fontweight='bold')
ax1.set_ylabel("Percentual")
ax1.set_xlabel("")
ax1.legend(title="Safra", bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=8)
ax1.tick_params(axis='x', rotation=45)

# 2. Boxplot idade por região
ax2 = axes[0, 1]
regioes_order = [r[1] for r in BASES]
df_brasil.boxplot(column="IDADE_ANOS", by="REGIAO", ax=ax2, positions=range(len(regioes_order)))
ax2.set_title("Distribuição de Idade por Região", fontweight='bold')
ax2.set_xlabel("")
ax2.set_ylabel("Idade (anos)")
plt.suptitle("")
ax2.tick_params(axis='x', rotation=45)

# 3. Histograma de valores (log)
ax3 = axes[0, 2]
df_brasil["VALOR_LOG"] = np.log10(df_brasil["VALOR_CONSOLIDADO"].clip(lower=1))
ax3.hist(df_brasil["VALOR_LOG"], bins=50, color='#3498db', alpha=0.7, edgecolor='white')
ax3.axvline(np.log10(1_000_000), color='red', linestyle='--', linewidth=2, label='R$ 1 mi')
ax3.axvline(np.log10(5_000_000), color='orange', linestyle='--', linewidth=2, label='R$ 5 mi')
ax3.set_title("Distribuição de Valores (log10)", fontweight='bold')
ax3.set_xlabel("log10(Valor)")
ax3.set_ylabel("Frequência")
ax3.legend()

# 4. Safra x Valor médio
ax4 = axes[1, 0]
safras_media = [s for s in ["2024-2025", "2023", "2022", "2021", "2020", "2019", "≤2018"] if s in df_brasil["SAFRA"].unique()]
safra_media = df_brasil.groupby("SAFRA")["VALOR_CONSOLIDADO"].mean().reindex(safras_media) / 1e6
bars = ax4.bar(safra_media.index, safra_media.values, color=[cores_safras[s] for s in safra_media.index])
ax4.set_title("Valor Médio por Safra (R$ milhões)", fontweight='bold')
ax4.set_ylabel("R$ milhões")
ax4.tick_params(axis='x', rotation=45)
for bar, val in zip(bars, safra_media.values):
    ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{val:.1f}', ha='center', va='bottom', fontsize=9)

# 5. CNPJs por classificação de ticket
ax5 = axes[1, 1]
class_counts = cnpj_agg["CLASSIFICACAO"].value_counts()
colors_class = ['#e74c3c', '#2ecc71', '#f39c12']
ax5.pie(class_counts.values, labels=None, autopct='%1.1f%%', colors=colors_class, startangle=90)
ax5.legend(class_counts.index, loc='center left', bbox_to_anchor=(1, 0.5), fontsize=9)
ax5.set_title("Classificação de CNPJs\n(Regra Ticket R$ 1 mi)", fontweight='bold')

# 6. Volume por safra (filtrado ≥ R$ 1mi)
ax6 = axes[1, 2]
if not safra_filtrado.empty:
    safra_vol = safra_filtrado["DIVIDA"].dropna() / 1e12
    bars = ax6.bar(safra_vol.index, safra_vol.values, color=[cores_safras.get(s, '#95a5a6') for s in safra_vol.index])
    ax6.set_title("Dívida por Safra (≥ R$ 1 mi, R$ tri)", fontweight='bold')
    ax6.set_ylabel("R$ trilhões")
    ax6.tick_params(axis='x', rotation=45)
    for bar, val in zip(bars, safra_vol.values):
        ax6.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{val:.1f}', ha='center', va='bottom', fontsize=9)

plt.suptitle("ANÁLISE DE HIPÓTESES — DISTRIBUIÇÃO TEMPORAL E TICKET", fontsize=14, fontweight='bold', y=1.02)
plt.tight_layout()
plt.savefig("analise_hipoteses_safra_ticket.png", dpi=150, bbox_inches='tight')
plt.close()
print("✅ Gráfico salvo: analise_hipoteses_safra_ticket.png")

# ================= EXPORTAR RESUMO =================
cnpj_agg.to_csv("cnpjs_classificados_por_ticket.csv", sep=";", index=False, encoding="utf-8-sig")
print("✅ CSV salvo: cnpjs_classificados_por_ticket.csv")

cross_pct.to_csv("distribuicao_safra_por_regiao_pct.csv", sep=";", encoding="utf-8-sig")
print("✅ CSV salvo: distribuicao_safra_por_regiao_pct.csv")

print("\n" + "=" * 100)
print("🏁 ANÁLISE DE HIPÓTESES CONCLUÍDA!")
print("=" * 100)
