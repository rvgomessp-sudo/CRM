import pandas as pd

# ================= CONFIG =================
ARQUIVO = "arquivo_lai_SIDA_1_202512.csv"

# Tributos estruturais de empresas médio / grande porte
TRIBUTOS_ALVO = [
    "COFINS",
    "PIS",
    "IRPJ",
    "CSLL",
    "IPI",
    "IOF",
    "Imposto de Importação"
]

# Termos explicitamente excluídos (ruído / fora de escopo)
EXCLUIR_TERMOS = [
    "SIMPLES",
    "MEI",
    "IRRF",
    "MULTA",
    "CUSTAS"
]

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

print("📥 Lendo base PGFN...")
df = pd.read_csv(
    ARQUIVO,
    sep=";",
    encoding="latin1",
    usecols=COLUNAS,
    low_memory=False
)

# ================= LIMPEZA =================
df["VALOR_CONSOLIDADO"] = (
    df["VALOR_CONSOLIDADO"]
    .astype(str)
    .str.replace(".", "", regex=False)
    .str.replace(",", ".", regex=False)
    .astype(float)
)

df["DATA_INSCRICAO"] = pd.to_datetime(
    df["DATA_INSCRICAO"],
    errors="coerce",
    dayfirst=True
)

df["ANO_INSCRICAO"] = df["DATA_INSCRICAO"].dt.year

# ================= FILTRO POSITIVO =================
mask_tributos = df["RECEITA_PRINCIPAL"].str.contains(
    "|".join(TRIBUTOS_ALVO),
    case=False,
    na=False
)

# ================= FILTRO NEGATIVO =================
mask_excluir = df["RECEITA_PRINCIPAL"].str.contains(
    "|".join(EXCLUIR_TERMOS),
    case=False,
    na=False
)

df = df[mask_tributos & ~mask_excluir]

print(f"🎯 Registros após filtro estratégico: {len(df):,}")

# ================= 1. AGREGADO POR TRIBUTO =================
print("\n📊 TRIBUTOS — INSCRIÇÕES, DÍVIDA TOTAL, MÉDIA, CNPJs")
tributos = (
    df.groupby("RECEITA_PRINCIPAL")
    .agg(
        QTD_INSCRICOES=("NUMERO_INSCRICAO", "count"),
        DIVIDA_TOTAL=("VALOR_CONSOLIDADO", "sum"),
        DIVIDA_MEDIA=("VALOR_CONSOLIDADO", "mean"),
        CNPJS_DISTINTOS=("CPF_CNPJ", "nunique")
    )
    .sort_values("DIVIDA_TOTAL", ascending=False)
)

print(tributos)

# ================= 2. DISTRIBUIÇÃO POR ANO =================
print("\n📅 DISTRIBUIÇÃO DA DÍVIDA POR ANO (R$)")
ano = (
    df.groupby("ANO_INSCRICAO")["VALOR_CONSOLIDADO"]
    .sum()
    .sort_index()
)

print(ano)

# ================= 3. TOP 10 EMPRESAS =================
print("\n🏢 TOP 10 EMPRESAS — MAIOR DÍVIDA CONSOLIDADA")
top_empresas = (
    df.groupby(["CPF_CNPJ", "NOME_DEVEDOR"])
    .agg(
        DIVIDA_TOTAL=("VALOR_CONSOLIDADO", "sum"),
        QTD_INSCRICOES=("NUMERO_INSCRICAO", "count")
    )
    .sort_values("DIVIDA_TOTAL", ascending=False)
    .head(10)
)

print(top_empresas)

# ================= 4. AJUIZADO x NÃO AJUIZADO =================
print("\n⚖️ AJUIZADO x NÃO AJUIZADO — POR VALOR E SITUAÇÃO")
ajuizado = (
    df.groupby(["INDICADOR_AJUIZADO", "SITUACAO_INSCRICAO"])
    .agg(
        DIVIDA_TOTAL=("VALOR_CONSOLIDADO", "sum"),
        QTD_INSCRICOES=("NUMERO_INSCRICAO", "count")
    )
    .sort_values("DIVIDA_TOTAL", ascending=False)
)

print(ajuizado)

print("\n✅ ANÁLISE ESTRATÉGICA CONCLUÍDA")