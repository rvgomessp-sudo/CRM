import pandas as pd
import os

# ================= CONFIG =================
os.chdir(r"C:\Rodrigo\BasePGFN\2025")
ARQUIVO = "arquivo_lai_SIDA_1_202512.csv"

# Tributos estruturais de empresas médio/grande porte
TRIBUTOS_ALVO = [
    "COFINS",
    "PIS", 
    "IRPJ",
    "CSLL",
    "IPI",
    "IOF",
    "Imposto de Importação"
]

# Excluir ruído / pequeno porte
EXCLUIR_TERMOS = [
    "SIMPLES",
    "MEI",
    "IRRF",
    "MULTA",
    "CUSTAS"
]

# Colunas necessárias
COLUNAS = [
    "CPF_CNPJ",
    "TIPO_PESSOA",
    "TIPO_DEVEDOR",
    "NOME_DEVEDOR",
    "UF_DEVEDOR",
    "UNIDADE_RESPONSAVEL",
    "NUMERO_INSCRICAO",
    "TIPO_SITUACAO_INSCRICAO",
    "SITUACAO_INSCRICAO",
    "RECEITA_PRINCIPAL",
    "DATA_INSCRICAO",
    "INDICADOR_AJUIZADO",
    "VALOR_CONSOLIDADO"
]

# ================= FUNÇÃO FORMATAR BRL =================
def fmt_brl(valor):
    if pd.isna(valor):
        return "R$ -"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# ================= LEITURA =================
print("=" * 70)
print("📥 LENDO BASE PGFN COMPLETA...")
print("=" * 70)

df = pd.read_csv(
    ARQUIVO,
    sep=";",
    encoding="latin1",
    usecols=COLUNAS,
    low_memory=False
)

print(f"Total registros brutos: {len(df):,}")

# ================= FILTRO 1: PESSOA JURÍDICA =================
# CORREÇÃO: usar "jur" (antes do acento) com case=False
pj_mask = df["TIPO_PESSOA"].astype(str).str.contains("jur", case=False, na=False)
df = df[pj_mask].copy()
print(f"\n🏢 Filtro PJ (Pessoa Jurídica): {len(df):,} registros")

if len(df) == 0:
    print("❌ ERRO: Filtro PJ retornou zero registros!")
    print("Valores únicos de TIPO_PESSOA:")
    df_check = pd.read_csv(ARQUIVO, sep=";", encoding="latin1", usecols=["TIPO_PESSOA"], nrows=1000)
    print(df_check["TIPO_PESSOA"].value_counts())
    raise SystemExit("Verifique a coluna TIPO_PESSOA")

# ================= FILTRO 2: TRIBUTOS ESTRUTURAIS =================
mask_tributos = df["RECEITA_PRINCIPAL"].astype(str).str.contains("|".join(TRIBUTOS_ALVO), case=False, na=False)
mask_excluir = df["RECEITA_PRINCIPAL"].astype(str).str.contains("|".join(EXCLUIR_TERMOS), case=False, na=False)
df = df[mask_tributos & ~mask_excluir].copy()
print(f"🎯 Filtro tributos médio/grande porte: {len(df):,} registros")

if len(df) == 0:
    raise SystemExit("❌ ERRO: Filtro de tributos retornou zero registros!")

# ================= CONVERSÃO VALOR PARA REAIS =================
# Padrão brasileiro: 1.234.567,89 → remover pontos, trocar vírgula por ponto
df["VALOR_CONSOLIDADO"] = (
    df["VALOR_CONSOLIDADO"]
    .astype(str)
    .str.replace(".", "", regex=False)
    .str.replace(",", ".", regex=False)
)
df["VALOR_CONSOLIDADO"] = pd.to_numeric(df["VALOR_CONSOLIDADO"], errors="coerce")

# ================= RESUMO GERAL =================
print("\n" + "=" * 70)
print("📊 RESUMO GERAL - PJ + TRIBUTOS MÉDIO/GRANDE PORTE")
print("=" * 70)

total_divida = df["VALOR_CONSOLIDADO"].sum()
media_divida = df["VALOR_CONSOLIDADO"].mean()
mediana_divida = df["VALOR_CONSOLIDADO"].median()

print(f"\n📌 VOLUME:")
print(f"   Registros (inscrições): {len(df):,}")
print(f"   CNPJs únicos: {df['CPF_CNPJ'].nunique():,}")

print(f"\n📌 DÍVIDA TOTAL:")
print(f"   {fmt_brl(total_divida)}")

print(f"\n📌 DÍVIDA MÉDIA por inscrição:")
print(f"   {fmt_brl(media_divida)}")

print(f"\n📌 DÍVIDA MEDIANA por inscrição:")
print(f"   {fmt_brl(mediana_divida)}")

# ================= POR TRIBUTO =================
print("\n" + "=" * 70)
print("📊 VOLUME POR TRIBUTO")
print("=" * 70)

por_tributo = (
    df.groupby("RECEITA_PRINCIPAL")
    .agg(
        QTD=("NUMERO_INSCRICAO", "count"),
        DIVIDA_TOTAL=("VALOR_CONSOLIDADO", "sum"),
        DIVIDA_MEDIA=("VALOR_CONSOLIDADO", "mean"),
        CNPJS=("CPF_CNPJ", "nunique")
    )
    .sort_values("DIVIDA_TOTAL", ascending=False)
)

print(f"\n{'TRIBUTO':<60} {'QTD':>10} {'DÍVIDA TOTAL':>22} {'CNPJs':>10}")
print("-" * 105)
for idx, row in por_tributo.head(20).iterrows():
    tributo_nome = idx[:60] if len(idx) > 60 else idx
    print(f"{tributo_nome:<60} {int(row['QTD']):>10,} {fmt_brl(row['DIVIDA_TOTAL']):>22} {int(row['CNPJS']):>10,}")

print("-" * 105)
print(f"{'TOTAL':<60} {len(df):>10,} {fmt_brl(total_divida):>22} {df['CPF_CNPJ'].nunique():>10,}")

# ================= POR SITUAÇÃO =================
print("\n" + "=" * 70)
print("📊 TOP 20 SITUAÇÕES (por dívida)")
print("=" * 70)

por_situacao = (
    df.groupby("SITUACAO_INSCRICAO")
    .agg(
        QTD=("NUMERO_INSCRICAO", "count"),
        DIVIDA_TOTAL=("VALOR_CONSOLIDADO", "sum")
    )
    .sort_values("DIVIDA_TOTAL", ascending=False)
    .head(20)
)

print(f"\n{'SITUAÇÃO':<65} {'QTD':>10} {'DÍVIDA TOTAL':>22}")
print("-" * 100)
for idx, row in por_situacao.iterrows():
    sit_nome = idx[:65] if len(idx) > 65 else idx
    print(f"{sit_nome:<65} {int(row['QTD']):>10,} {fmt_brl(row['DIVIDA_TOTAL']):>22}")

# ================= AJUIZADO x NÃO AJUIZADO =================
print("\n" + "=" * 70)
print("📊 AJUIZADO x NÃO AJUIZADO")
print("=" * 70)

df["AJUIZADO_BIN"] = df["INDICADOR_AJUIZADO"].astype(str).str.upper().isin(["SIM", "S", "1", "TRUE"])

ajuizado_stats = df.groupby("AJUIZADO_BIN").agg(
    QTD=("NUMERO_INSCRICAO", "count"),
    DIVIDA_TOTAL=("VALOR_CONSOLIDADO", "sum"),
    CNPJS=("CPF_CNPJ", "nunique")
)

print(f"\n{'STATUS':<20} {'QTD':>12} {'DÍVIDA TOTAL':>25} {'CNPJs':>12}")
print("-" * 72)
for idx, row in ajuizado_stats.iterrows():
    status = "AJUIZADO" if idx else "NÃO AJUIZADO"
    print(f"{status:<20} {int(row['QTD']):>12,} {fmt_brl(row['DIVIDA_TOTAL']):>25} {int(row['CNPJS']):>12,}")

# ================= GARANTIA =================
print("\n" + "=" * 70)
print("📊 TIPO DE GARANTIA")
print("=" * 70)

def extrair_garantia(s):
    s = str(s).upper()
    if "SEGURO GARANTIA" in s:
        return "SEGURO GARANTIA"
    if "PENHORA" in s:
        return "PENHORA"
    if "CARTA FIANCA" in s or "CARTA FIANÇA" in s:
        return "CARTA FIANÇA"
    if "DEPOSITO" in s or "DEPÓSITO" in s:
        return "DEPÓSITO"
    return "SEM GARANTIA"

df["GARANTIA_ATUAL"] = df["SITUACAO_INSCRICAO"].apply(extrair_garantia)

garantia_stats = df.groupby("GARANTIA_ATUAL").agg(
    QTD=("NUMERO_INSCRICAO", "count"),
    DIVIDA_TOTAL=("VALOR_CONSOLIDADO", "sum"),
    CNPJS=("CPF_CNPJ", "nunique")
).sort_values("DIVIDA_TOTAL", ascending=False)

print(f"\n{'GARANTIA':<20} {'QTD':>12} {'DÍVIDA TOTAL':>25} {'CNPJs':>12} {'%':>8}")
print("-" * 80)
for idx, row in garantia_stats.iterrows():
    pct = (row['QTD'] / len(df)) * 100
    print(f"{idx:<20} {int(row['QTD']):>12,} {fmt_brl(row['DIVIDA_TOTAL']):>25} {int(row['CNPJS']):>12,} {pct:>7.2f}%")

# ================= CRUZAMENTO: AJUIZADO x GARANTIA =================
print("\n" + "=" * 70)
print("📊 CRUZAMENTO: AJUIZADO × GARANTIA")
print("=" * 70)

cross = pd.crosstab(
    df["AJUIZADO_BIN"].map({True: "AJUIZADO", False: "NÃO AJUIZADO"}),
    df["GARANTIA_ATUAL"],
    values=df["VALOR_CONSOLIDADO"],
    aggfunc="sum",
    margins=True
)

print("\nDÍVIDA TOTAL (R$):")
print(cross.applymap(lambda x: fmt_brl(x) if pd.notna(x) else "-"))

cross_qtd = pd.crosstab(
    df["AJUIZADO_BIN"].map({True: "AJUIZADO", False: "NÃO AJUIZADO"}),
    df["GARANTIA_ATUAL"],
    margins=True
)

print("\nQUANTIDADE DE INSCRIÇÕES:")
print(cross_qtd)

# ================= MERCADO ALVO: AJUIZADO + SEM GARANTIA =================
print("\n" + "=" * 70)
print("🎯 MERCADO ALVO: AJUIZADO + SEM GARANTIA")
print("=" * 70)

mercado_alvo = df[(df["AJUIZADO_BIN"] == True) & (df["GARANTIA_ATUAL"] == "SEM GARANTIA")]

print(f"\n📌 VOLUME:")
print(f"   Inscrições: {len(mercado_alvo):,}")
print(f"   CNPJs únicos: {mercado_alvo['CPF_CNPJ'].nunique():,}")
print(f"   Dívida total: {fmt_brl(mercado_alvo['VALOR_CONSOLIDADO'].sum())}")
print(f"   Dívida média: {fmt_brl(mercado_alvo['VALOR_CONSOLIDADO'].mean())}")
print(f"   Dívida mediana: {fmt_brl(mercado_alvo['VALOR_CONSOLIDADO'].median())}")

if len(mercado_alvo) > 0:
    print(f"\n📌 TOP 10 TRIBUTOS (AJUIZADO + SEM GARANTIA):")
    top_trib = mercado_alvo.groupby("RECEITA_PRINCIPAL").agg(
        DIVIDA=("VALOR_CONSOLIDADO", "sum")
    ).sort_values("DIVIDA", ascending=False).head(10)
    
    for idx, row in top_trib.iterrows():
        print(f"   {idx[:50]}: {fmt_brl(row['DIVIDA'])}")

print("\n" + "=" * 70)
print("🏁 ANÁLISE CONCLUÍDA!")
print("=" * 70)