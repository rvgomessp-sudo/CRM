import pandas as pd
from datetime import datetime

# =============================
# CONFIGURAÇÕES
# =============================
ENTRADA = r"C:\Rodrigo\BasePGFN\2025\amostra_grande_pgfn.csv"
SAIDA   = r"C:\Rodrigo\BasePGFN\2025\cnpj_consolidado_classificado_final.csv"

print("📥 Lendo base original...")
df = pd.read_csv(
    ENTRADA,
    sep=";",
    low_memory=False
)

print(f"📊 Linhas lidas: {len(df)}")

# =============================
# LIMPEZA DE DOCUMENTO (CPF / CNPJ)
# =============================
print("🧹 Limpando CPF/CNPJ...")

df["DOC_LIMPO"] = (
    df["CPF_CNPJ"]
    .astype(str)
    .str.replace(r"\D", "", regex=True)
)

# mantém apenas CNPJ real (14 dígitos)
df = df[df["DOC_LIMPO"].str.len() == 14]

print(f"🏢 Linhas após filtro CNPJ: {len(df)}")
print(f"🏢 CNPJs distintos: {df['DOC_LIMPO'].nunique()}")

# =============================
# TRATAMENTO DE VALOR
# =============================
print("💰 Tratando VALOR_CONSOLIDADO...")

df["VALOR_CONSOLIDADO"] = (
    df["VALOR_CONSOLIDADO"]
    .astype(str)
    .str.replace(".", "", regex=False)
    .str.replace(",", ".", regex=False)
)

df["VALOR_CONSOLIDADO"] = pd.to_numeric(
    df["VALOR_CONSOLIDADO"],
    errors="coerce"
)

df = df[df["VALOR_CONSOLIDADO"].notna()]

# =============================
# DATAS E FLAGS JURÍDICAS
# =============================
print("📅 Processando datas e flags jurídicas...")

df["DATA_INSCRICAO"] = pd.to_datetime(
    df["DATA_INSCRICAO"],
    errors="coerce",
    dayfirst=True
)

hoje = pd.Timestamp(datetime.today().date())
df["IDADE_DIAS"] = (hoje - df["DATA_INSCRICAO"]).dt.days

df["FLAG_AJUIZADA"] = (
    df["INDICADOR_AJUIZADO"]
    .astype(str)
    .str.upper()
    .isin(["SIM", "S"])
)

df["FLAG_EM_COBRANCA"] = (
    df["SITUACAO_INSCRICAO"]
    .astype(str)
    .str.upper()
    .str.contains("COBRANCA", na=False)
)

# =============================
# CONSOLIDAÇÃO POR CNPJ
# =============================
print("📊 Consolidando por CNPJ...")

consolidado = (
    df.groupby("DOC_LIMPO")
      .agg(
          VALOR_TOTAL=("VALOR_CONSOLIDADO", "sum"),
          QTD_INSCRICOES=("VALOR_CONSOLIDADO", "count"),
          DIVIDA_MAIS_ANTIGA=("IDADE_DIAS", "max"),
          DIVIDA_MAIS_RECENTE=("IDADE_DIAS", "min"),
          TEM_AJUIZAMENTO=("FLAG_AJUIZADA", "any"),
          TEM_EM_COBRANCA=("FLAG_EM_COBRANCA", "any")
      )
      .reset_index()
)

# =============================
# CLASSIFICAÇÃO ESTRATÉGICA
# =============================
print("🧠 Classificando empresas...")

def classificar(row):
    if row["VALOR_TOTAL"] >= 15_000_000 and not row["TEM_AJUIZAMENTO"]:
        return "🔴 ALTA PRIORIDADE – SEGURO GARANTIA"
    if row["VALOR_TOTAL"] >= 15_000_000 and row["TEM_AJUIZAMENTO"]:
        return "🟠 SUBSTITUIÇÃO / ESTRATÉGIA JUDICIAL"
    if row["VALOR_TOTAL"] >= 1_000_000 and row["QTD_INSCRICOES"] >= 5:
        return "🟡 ESTRATÉGICA – CONSOLIDAÇÃO"
    return "⚪ CAUDA / FUNIL"

consolidado["CLASSIFICACAO"] = consolidado.apply(classificar, axis=1)

# =============================
# RELATÓRIOS NO TERMINAL
# =============================
print("\n📈 Distribuição por classificação:")
print(consolidado["CLASSIFICACAO"].value_counts())

print("\n💰 Top 10 maiores passivos:")
print(
    consolidado
    .sort_values("VALOR_TOTAL", ascending=False)
    .head(10)[
        ["DOC_LIMPO", "VALOR_TOTAL", "QTD_INSCRICOES", "CLASSIFICACAO"]
    ]
)

# =============================
# EXPORTAÇÃO FINAL
# =============================
consolidado.to_csv(
    SAIDA,
    sep=";",
    index=False,
    decimal=","
)

print("\n✅ PIPELINE FINAL CONCLUÍDO COM SUCESSO")
print(f"📄 Arquivo gerado: {SAIDA}")
print(f"🏢 Empresas consolidadas: {len(consolidado)}")