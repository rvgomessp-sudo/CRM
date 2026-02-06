"""
Unificacao das 6 bases PGFN - Base Nacional Semi-Bruta

Gera uma base consolidada com TODAS as inscricoes de:
- Pessoa Juridica (TIPO_PESSOA)
- Devedor Principal (TIPO_DEVEDOR)

Preserva todas as colunas originais, sem agregacao.
Cada linha = 1 inscricao valida.

Saida: pgfn_unificada_cnpj_principal_semibruta.csv
"""

import pandas as pd
import sys

import config
from utils import ler_base_pgfn, converter_valor_brl, salvar_csv, fmt_brl


def formatar_valor_br(valor):
    """Formata valor numerico no padrao brasileiro (ponto milhar, virgula decimal)."""
    if pd.isna(valor):
        return ""
    return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def ler_e_filtrar_base(arquivo, regiao_nome):
    """Le uma base e aplica filtros: PJ + PRINCIPAL."""
    print(f"\n  Lendo {arquivo}...")

    try:
        # Ler TODAS as colunas
        df = ler_base_pgfn(arquivo, colunas=None)
    except FileNotFoundError as e:
        print(f"    AVISO: {e}")
        return None

    total_bruto = len(df)
    print(f"    Total bruto: {total_bruto:,}")

    # Filtro 1: TIPO_PESSOA == "Pessoa jurídica"
    mask_pj = df["TIPO_PESSOA"].astype(str).str.strip().str.lower() == "pessoa jurídica"
    df_pj = df[mask_pj].copy()
    print(f"    Apos filtro PJ: {len(df_pj):,}")

    # Filtro 2: TIPO_DEVEDOR == "PRINCIPAL"
    mask_principal = df_pj["TIPO_DEVEDOR"].astype(str).str.strip().str.upper() == "PRINCIPAL"
    df_filtrado = df_pj[mask_principal].copy()
    print(f"    Apos filtro PRINCIPAL: {len(df_filtrado):,}")

    # Adicionar coluna de regiao (referencia)
    df_filtrado["REGIAO_SIDA"] = regiao_nome

    return df_filtrado


def criar_colunas_valor(df):
    """Cria colunas de valor numerico e formatado BR."""
    df = df.copy()

    # VALOR_CONSOLIDADO_NUM (float)
    df["VALOR_CONSOLIDADO_NUM"] = converter_valor_brl(df["VALOR_CONSOLIDADO"])

    # VALOR_CONSOLIDADO_BR (string formatada)
    df["VALOR_CONSOLIDADO_BR"] = df["VALOR_CONSOLIDADO_NUM"].apply(formatar_valor_br)

    return df


def imprimir_resumo(df):
    """Imprime resumo no terminal."""
    print("\n" + "=" * 80)
    print("  RESUMO DA BASE UNIFICADA")
    print("=" * 80)

    total_inscricoes = len(df)
    total_cnpjs = df["CPF_CNPJ"].nunique()
    valor_total = df["VALOR_CONSOLIDADO_NUM"].sum()

    print(f"\n  Total de inscricoes: {total_inscricoes:,}")
    print(f"  Total de CNPJs unicos: {total_cnpjs:,}")
    print(f"  Valor total consolidado: {fmt_brl(valor_total)}")
    print(f"  Valor total (numerico): R$ {valor_total:,.2f}")

    # Estatisticas adicionais
    print(f"\n  ESTATISTICAS DE VALOR:")
    print(f"    Media: {fmt_brl(df['VALOR_CONSOLIDADO_NUM'].mean())}")
    print(f"    Mediana: {fmt_brl(df['VALOR_CONSOLIDADO_NUM'].median())}")
    print(f"    Minimo: {fmt_brl(df['VALOR_CONSOLIDADO_NUM'].min())}")
    print(f"    Maximo: {fmt_brl(df['VALOR_CONSOLIDADO_NUM'].max())}")

    # Por regiao
    print(f"\n  POR REGIAO:")
    por_regiao = df.groupby("REGIAO_SIDA").agg(
        INSCRICOES=("NUMERO_INSCRICAO", "count"),
        CNPJS=("CPF_CNPJ", "nunique"),
        VALOR=("VALOR_CONSOLIDADO_NUM", "sum"),
    ).sort_values("VALOR", ascending=False)

    print(f"  {'REGIAO':<40} {'INSCR':>10} {'CNPJs':>10} {'VALOR':>18}")
    print("  " + "-" * 80)
    for regiao, row in por_regiao.iterrows():
        print(f"  {regiao:<40} {row['INSCRICOES']:>10,} {row['CNPJS']:>10,} {fmt_brl(row['VALOR']):>18}")

    # Top 10 inscricoes por valor
    print(f"\n  TOP 10 INSCRICOES POR VALOR:")
    print(f"  {'#':>3} {'CNPJ':<18} {'NOME':<35} {'UF':>4} {'VALOR':>18}")
    print("  " + "-" * 85)

    top10 = df.nlargest(10, "VALOR_CONSOLIDADO_NUM")
    for i, (_, row) in enumerate(top10.iterrows(), 1):
        nome = str(row.get("NOME_DEVEDOR", ""))[:33]
        uf = str(row.get("UF_DEVEDOR", ""))
        cnpj = str(row.get("CPF_CNPJ", ""))
        valor = row["VALOR_CONSOLIDADO_NUM"]
        print(f"  {i:>3} {cnpj:<18} {nome:<35} {uf:>4} {fmt_brl(valor):>18}")


def main():
    print("=" * 80)
    print("  UNIFICACAO DAS 6 BASES PGFN - BASE NACIONAL SEMI-BRUTA")
    print("=" * 80)
    print(f"  Diretorio de dados: {config.DATA_DIR}")
    print(f"\n  Filtros aplicados:")
    print(f"    - TIPO_PESSOA == 'Pessoa juridica'")
    print(f"    - TIPO_DEVEDOR == 'PRINCIPAL'")

    # Processar todas as 6 bases
    partes = []
    for arquivo, nome_curto, nome_completo in config.BASES:
        df = ler_e_filtrar_base(arquivo, nome_completo)
        if df is not None and len(df) > 0:
            partes.append(df)

    if not partes:
        print("\nERRO: Nenhuma inscricao encontrada nas bases.")
        sys.exit(1)

    # Concatenar em DataFrame nacional
    print("\n" + "-" * 80)
    print("  CONCATENANDO BASES...")
    df_nacional = pd.concat(partes, ignore_index=True)
    print(f"  Total concatenado: {len(df_nacional):,} inscricoes")

    # Criar colunas de valor
    print("\n  Criando colunas de valor...")
    df_nacional = criar_colunas_valor(df_nacional)

    # Imprimir resumo
    imprimir_resumo(df_nacional)

    # Exportar CSV
    print("\n" + "-" * 80)
    print("  EXPORTANDO CSV...")

    # Para o CSV, manter VALOR_CONSOLIDADO_NUM como numerico (ponto decimal)
    csv_path = config.get_output_path("pgfn_unificada_cnpj_principal_semibruta.csv")

    # Formatar VALOR_CONSOLIDADO_NUM com ponto decimal no CSV
    df_export = df_nacional.copy()
    df_export["VALOR_CONSOLIDADO_NUM"] = df_export["VALOR_CONSOLIDADO_NUM"].apply(
        lambda x: f"{x:.2f}" if pd.notna(x) else ""
    )

    df_export.to_csv(csv_path, sep=";", index=False, encoding="utf-8-sig")
    print(f"  Arquivo: {csv_path}")
    print(f"  Linhas: {len(df_nacional):,}")
    print(f"  Colunas: {len(df_nacional.columns)}")

    # Listar colunas
    print(f"\n  COLUNAS NO ARQUIVO:")
    for i, col in enumerate(df_nacional.columns, 1):
        print(f"    {i:>2}. {col}")

    print("\n" + "=" * 80)
    print("  UNIFICACAO CONCLUIDA!")
    print("=" * 80)


if __name__ == "__main__":
    main()
