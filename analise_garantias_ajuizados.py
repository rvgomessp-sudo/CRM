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
print("=" * 80)
print("📥 LENDO BASE PGFN COMPLETA...")
print("=" * 80)

df = pd.read_csv(
    ARQUIVO,
    sep=";",
    encoding="latin1",
    usecols=COLUNAS,
    low_memory=False
)

print(f"Total registros brutos: {len(df):,}")

# ================= FILTRO 1: PESSOA JURÍDICA =================
pj_mask = df["TIPO_PESSOA"].astype(str).str.contains("jur", case=False, na=False)
df = df[pj_mask].copy()
print(f"🏢 Filtro PJ: {len(df):,} registros")

# ================= FILTRO 2: TRIBUTOS ESTRUTURAIS =================
mask_tributos = df["RECEITA_PRINCIPAL"].astype(str).str.contains("|".join(TRIBUTOS_ALVO), case=False, na=False)
mask_excluir = df["RECEITA_PRINCIPAL"].astype(str).str.contains("|".join(EXCLUIR_TERMOS), case=False, na=False)
df = df[mask_tributos & ~mask_excluir].copy()
print(f"🎯 Filtro tributos: {len(df):,} registros")

# ================= CONVERSÃO VALOR =================
df["VALOR_CONSOLIDADO"] = (
    df["VALOR_CONSOLIDADO"]
    .astype(str)
    .str.replace(".", "", regex=False)
    .str.replace(",", ".", regex=False)
)
df["VALOR_CONSOLIDADO"] = pd.to_numeric(df["VALOR_CONSOLIDADO"], errors="coerce")

# ================= FEATURES =================
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
df["AJUIZADO_BIN"] = df["INDICADOR_AJUIZADO"].astype(str).str.upper().isin(["SIM", "S", "1", "TRUE"])

# ================= FILTRO: SOMENTE AJUIZADOS =================
df_ajuizado = df[df["AJUIZADO_BIN"] == True].copy()
print(f"\n⚖️ Total AJUIZADOS: {len(df_ajuizado):,} registros")

# ================= ANÁLISE POR TIPO DE GARANTIA (AJUIZADOS) =================
print("\n" + "=" * 100)
print("📊 AJUIZADOS — ESTATÍSTICAS POR TIPO DE GARANTIA")
print("=" * 100)

# Ordem de exibição
ordem_garantia = ["SEGURO GARANTIA", "CARTA FIANÇA", "DEPÓSITO", "PENHORA", "SEM GARANTIA"]

print(f"\n{'GARANTIA':<20} {'QTD':>12} {'CNPJs':>10} {'DÍVIDA TOTAL':>25} {'MÉDIA':>20} {'MEDIANA':>20}")
print("-" * 110)

resultados = []

for garantia in ordem_garantia:
    subset = df_ajuizado[df_ajuizado["GARANTIA_ATUAL"] == garantia]
    
    if len(subset) > 0:
        qtd = len(subset)
        cnpjs = subset["CPF_CNPJ"].nunique()
        divida_total = subset["VALOR_CONSOLIDADO"].sum()
        media = subset["VALOR_CONSOLIDADO"].mean()
        mediana = subset["VALOR_CONSOLIDADO"].median()
        
        resultados.append({
            "GARANTIA": garantia,
            "QTD": qtd,
            "CNPJs": cnpjs,
            "DIVIDA_TOTAL": divida_total,
            "MEDIA": media,
            "MEDIANA": mediana
        })
        
        print(f"{garantia:<20} {qtd:>12,} {cnpjs:>10,} {fmt_brl(divida_total):>25} {fmt_brl(media):>20} {fmt_brl(mediana):>20}")
    else:
        print(f"{garantia:<20} {'0':>12} {'-':>10} {'-':>25} {'-':>20} {'-':>20}")

print("-" * 110)

# Total ajuizados
total_qtd = len(df_ajuizado)
total_cnpjs = df_ajuizado["CPF_CNPJ"].nunique()
total_divida = df_ajuizado["VALOR_CONSOLIDADO"].sum()
total_media = df_ajuizado["VALOR_CONSOLIDADO"].mean()
total_mediana = df_ajuizado["VALOR_CONSOLIDADO"].median()

print(f"{'TOTAL AJUIZADOS':<20} {total_qtd:>12,} {total_cnpjs:>10,} {fmt_brl(total_divida):>25} {fmt_brl(total_media):>20} {fmt_brl(total_mediana):>20}")

# ================= COMPARATIVO VISUAL =================
print("\n" + "=" * 100)
print("📊 COMPARATIVO — MÉDIA vs MEDIANA (AJUIZADOS)")
print("=" * 100)

print(f"\n{'GARANTIA':<20} {'MÉDIA':>25} {'MEDIANA':>25} {'RATIO M/m':>15}")
print("-" * 90)

for r in resultados:
    ratio = r["MEDIA"] / r["MEDIANA"] if r["MEDIANA"] > 0 else 0
    print(f"{r['GARANTIA']:<20} {fmt_brl(r['MEDIA']):>25} {fmt_brl(r['MEDIANA']):>25} {ratio:>15.1f}x")

# ================= PERCENTIS POR GARANTIA =================
print("\n" + "=" * 100)
print("📊 PERCENTIS DE VALOR (AJUIZADOS) — P25, P50, P75, P90, P95")
print("=" * 100)

print(f"\n{'GARANTIA':<20} {'P25':>18} {'P50 (MED)':>18} {'P75':>18} {'P90':>18} {'P95':>18}")
print("-" * 115)

for garantia in ordem_garantia:
    subset = df_ajuizado[df_ajuizado["GARANTIA_ATUAL"] == garantia]["VALOR_CONSOLIDADO"]
    
    if len(subset) > 0:
        p25 = subset.quantile(0.25)
        p50 = subset.quantile(0.50)
        p75 = subset.quantile(0.75)
        p90 = subset.quantile(0.90)
        p95 = subset.quantile(0.95)
        
        print(f"{garantia:<20} {fmt_brl(p25):>18} {fmt_brl(p50):>18} {fmt_brl(p75):>18} {fmt_brl(p90):>18} {fmt_brl(p95):>18}")

print("\n" + "=" * 100)
print("🏁 ANÁLISE CONCLUÍDA!")
print("=" * 100)