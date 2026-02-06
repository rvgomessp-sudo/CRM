#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Busca em Cascata de Empresas e Devedores Solidários
====================================================
Localiza empresas por CNPJ ou nome, extrai todas as inscrições,
identifica devedores solidários e extrai também suas inscrições.

Empresas alvo:
- 10.998.286/0001-08 ; FR9 PARTICIPAÇÕES LTDA
- 10.998.322/0001-25 ; SF7 PARTICIPAÇÕES LTDA
"""

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
import re
import os

# Importa configurações do projeto
try:
    from config import DATA_DIR, BASES
except ImportError:
    DATA_DIR = Path(os.environ.get("PGFN_DATA_DIR", r"C:\Rodrigo\BasePGFN\2025"))
    BASES = [
        ("arquivo_lai_SIDA_1_202512.csv", "1a Regiao", "1a Região (DF/GO/MT/TO/Norte)"),
        ("arquivo_lai_SIDA_2_202512.csv", "2a Regiao", "2a Região (Nordeste)"),
        ("arquivo_lai_SIDA_3_202512.csv", "3a Regiao", "3a Região (SP Interior)"),
        ("arquivo_lai_SIDA_4_202512.csv", "4a Regiao", "4a Região (RS/SC)"),
        ("arquivo_lai_SIDA_5_202512.csv", "5a Regiao", "5a Região (PR/MS)"),
        ("arquivo_lai_SIDA_6_202512.csv", "6a Regiao", "6a Região (MG/RJ/ES/SP Capital)"),
    ]

# ============================================================================
# EMPRESAS ALVO
# ============================================================================
EMPRESAS_ALVO = [
    {
        "cnpj": "10.998.286/0001-08",
        "cnpj_limpo": "10998286000108",
        "cnpj_raiz": "10998286",
        "nome": "FR9 PARTICIPAÇÕES LTDA"
    },
    {
        "cnpj": "10.998.322/0001-25",
        "cnpj_limpo": "10998322000125",
        "cnpj_raiz": "10998322",
        "nome": "SF7 PARTICIPAÇÕES LTDA"
    }
]

# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def limpar_cnpj(cnpj):
    """Remove formatação do CNPJ, mantendo apenas dígitos."""
    if pd.isna(cnpj):
        return ""
    return re.sub(r"[^\d]", "", str(cnpj))

def formatar_cnpj(cnpj_limpo):
    """Formata CNPJ limpo para padrão XX.XXX.XXX/XXXX-XX."""
    cnpj = limpar_cnpj(cnpj_limpo)
    if len(cnpj) == 14:
        return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"
    return cnpj_limpo

def converter_valor_brl(valor_str):
    """Converte valor em formato BR (1.234.567,89) para float."""
    if pd.isna(valor_str):
        return 0.0
    valor = str(valor_str).strip()
    if valor == "" or valor == "-":
        return 0.0
    try:
        valor = valor.replace(".", "").replace(",", ".")
        return float(valor)
    except:
        return 0.0

def fmt_brl(valor):
    """Formata valor numérico para padrão brasileiro."""
    if pd.isna(valor) or valor == 0:
        return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def extrair_cnpj_raiz(cnpj):
    """Extrai os 8 primeiros dígitos do CNPJ (raiz)."""
    cnpj_limpo = limpar_cnpj(cnpj)
    if len(cnpj_limpo) >= 8:
        return cnpj_limpo[:8]
    return cnpj_limpo

# ============================================================================
# FUNÇÕES PRINCIPAIS
# ============================================================================

def carregar_base(arquivo, regiao):
    """Carrega uma base PGFN."""
    caminho = DATA_DIR / arquivo
    if not caminho.exists():
        print(f"  [!] Arquivo não encontrado: {caminho}")
        return None

    print(f"  Carregando {arquivo}...")
    df = pd.read_csv(caminho, sep=";", encoding="latin-1", dtype=str, low_memory=False)
    df["_REGIAO"] = regiao
    df["_ARQUIVO"] = arquivo

    # Criar coluna de CNPJ limpo para facilitar buscas
    if "CPF_CNPJ" in df.columns:
        df["_CNPJ_LIMPO"] = df["CPF_CNPJ"].apply(limpar_cnpj)
        df["_CNPJ_RAIZ"] = df["_CNPJ_LIMPO"].apply(lambda x: x[:8] if len(x) >= 8 else x)

    return df

def buscar_por_cnpj_ou_nome(df, cnpjs_limpos, cnpjs_raiz, nomes):
    """
    Busca registros por CNPJ completo, CNPJ raiz ou nome parcial.
    Retorna DataFrame com matches.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # Criar máscaras de busca
    mask_cnpj_completo = df["_CNPJ_LIMPO"].isin(cnpjs_limpos)
    mask_cnpj_raiz = df["_CNPJ_RAIZ"].isin(cnpjs_raiz)

    # Busca por nome (parcial, case insensitive)
    mask_nome = pd.Series([False] * len(df), index=df.index)
    if "NOME_DEVEDOR" in df.columns:
        nome_upper = df["NOME_DEVEDOR"].fillna("").str.upper()
        for nome in nomes:
            nome_busca = nome.upper()
            mask_nome = mask_nome | nome_upper.str.contains(nome_busca, regex=False, na=False)

    # Combinar máscaras
    mask_total = mask_cnpj_completo | mask_cnpj_raiz | mask_nome

    return df[mask_total].copy()

def identificar_inscricoes_vinculadas(df_encontrados):
    """
    A partir dos registros encontrados, identifica números de inscrição únicos.
    """
    if df_encontrados.empty:
        return set()

    if "NUMERO_INSCRICAO" in df_encontrados.columns:
        return set(df_encontrados["NUMERO_INSCRICAO"].dropna().unique())
    return set()

def buscar_solidarios_por_inscricao(df_base, inscricoes):
    """
    Busca todos os registros (devedores solidários) vinculados às mesmas inscrições.
    """
    if df_base is None or df_base.empty or not inscricoes:
        return pd.DataFrame()

    if "NUMERO_INSCRICAO" not in df_base.columns:
        return pd.DataFrame()

    mask = df_base["NUMERO_INSCRICAO"].isin(inscricoes)
    return df_base[mask].copy()

def identificar_novos_cnpjs(df_solidarios, cnpjs_ja_processados):
    """
    Identifica CNPJs que ainda não foram processados.
    """
    if df_solidarios.empty:
        return set(), {}

    novos = {}
    for _, row in df_solidarios.iterrows():
        cnpj_limpo = row.get("_CNPJ_LIMPO", "")
        if cnpj_limpo and cnpj_limpo not in cnpjs_ja_processados and len(cnpj_limpo) >= 8:
            nome = row.get("NOME_DEVEDOR", "DESCONHECIDO")
            tipo = row.get("TIPO_DEVEDOR", "N/A")
            novos[cnpj_limpo] = {"nome": nome, "tipo": tipo}

    return set(novos.keys()), novos

def executar_busca_cascata(bases_carregadas):
    """
    Executa busca em cascata:
    1. Busca empresas alvo
    2. Identifica inscrições
    3. Busca solidários nas mesmas inscrições
    4. Para cada novo CNPJ encontrado, busca suas outras inscrições
    5. Repete até não encontrar novos CNPJs
    """
    print("\n" + "=" * 70)
    print("BUSCA EM CASCATA")
    print("=" * 70)

    # Inicializar com empresas alvo
    cnpjs_processados = set()
    cnpjs_pendentes = set()
    cnpjs_raiz_pendentes = set()
    nomes_pendentes = []

    for emp in EMPRESAS_ALVO:
        cnpjs_pendentes.add(emp["cnpj_limpo"])
        cnpjs_raiz_pendentes.add(emp["cnpj_raiz"])
        nomes_pendentes.append(emp["nome"])

    # Mapeamento de CNPJs para informações
    info_cnpjs = {}
    for emp in EMPRESAS_ALVO:
        info_cnpjs[emp["cnpj_limpo"]] = {"nome": emp["nome"], "tipo": "ALVO", "origem": "EMPRESA ALVO"}

    # Acumular todos os registros encontrados
    todos_registros = []
    todas_inscricoes = set()

    iteracao = 0
    max_iteracoes = 10  # Limite de segurança

    while cnpjs_pendentes and iteracao < max_iteracoes:
        iteracao += 1
        print(f"\n--- Iteração {iteracao} ---")
        print(f"  CNPJs a buscar: {len(cnpjs_pendentes)}")

        # Buscar em todas as bases
        encontrados_iteracao = []

        for df_base in bases_carregadas:
            if df_base is None:
                continue

            # Busca por CNPJ e nome
            df_match = buscar_por_cnpj_ou_nome(
                df_base,
                cnpjs_pendentes,
                cnpjs_raiz_pendentes,
                nomes_pendentes
            )

            if not df_match.empty:
                encontrados_iteracao.append(df_match)

        if not encontrados_iteracao:
            print("  Nenhum registro encontrado nesta iteração.")
            break

        # Concatenar resultados
        df_encontrados = pd.concat(encontrados_iteracao, ignore_index=True)
        print(f"  Registros encontrados: {len(df_encontrados)}")

        # Identificar inscrições
        inscricoes_encontradas = identificar_inscricoes_vinculadas(df_encontrados)
        novas_inscricoes = inscricoes_encontradas - todas_inscricoes
        print(f"  Novas inscrições: {len(novas_inscricoes)}")

        todas_inscricoes.update(inscricoes_encontradas)

        # Marcar CNPJs como processados
        cnpjs_processados.update(cnpjs_pendentes)
        cnpjs_pendentes.clear()
        cnpjs_raiz_pendentes.clear()
        nomes_pendentes.clear()

        # Buscar TODOS os registros vinculados às inscrições (inclui solidários)
        if novas_inscricoes:
            for df_base in bases_carregadas:
                if df_base is None:
                    continue

                df_solidarios = buscar_solidarios_por_inscricao(df_base, todas_inscricoes)

                if not df_solidarios.empty:
                    todos_registros.append(df_solidarios)

                    # Identificar novos CNPJs
                    novos_cnpjs, novos_info = identificar_novos_cnpjs(df_solidarios, cnpjs_processados)

                    for cnpj, info in novos_info.items():
                        if cnpj not in info_cnpjs:
                            info_cnpjs[cnpj] = {
                                "nome": info["nome"],
                                "tipo": info["tipo"],
                                "origem": "SOLIDÁRIO/VINCULADO"
                            }

                    # Adicionar novos CNPJs para próxima iteração
                    cnpjs_pendentes.update(novos_cnpjs)
                    for cnpj in novos_cnpjs:
                        cnpjs_raiz_pendentes.add(cnpj[:8] if len(cnpj) >= 8 else cnpj)

        print(f"  Novos CNPJs identificados: {len(cnpjs_pendentes)}")

    # Consolidar resultados
    if todos_registros:
        df_final = pd.concat(todos_registros, ignore_index=True)
        # Remover duplicatas (mesmo registro pode vir de múltiplas iterações)
        cols_chave = ["NUMERO_INSCRICAO", "_CNPJ_LIMPO"]
        cols_existentes = [c for c in cols_chave if c in df_final.columns]
        if cols_existentes:
            df_final = df_final.drop_duplicates(subset=cols_existentes)
        print(f"\n  Total de registros únicos: {len(df_final)}")
    else:
        df_final = pd.DataFrame()
        print("\n  Nenhum registro encontrado!")

    return df_final, info_cnpjs

def processar_resultados(df, info_cnpjs):
    """
    Processa resultados, calcula valores consolidados e prepara para exportação.
    """
    if df.empty:
        return df

    # Converter valor consolidado
    if "VALOR_CONSOLIDADO" in df.columns:
        df["VALOR_CONSOLIDADO_NUM"] = df["VALOR_CONSOLIDADO"].apply(converter_valor_brl)
    else:
        df["VALOR_CONSOLIDADO_NUM"] = 0.0

    # Adicionar informações de classificação do CNPJ
    df["_CLASSIFICACAO"] = df["_CNPJ_LIMPO"].apply(
        lambda x: info_cnpjs.get(x, {}).get("origem", "DESCONHECIDO")
    )

    # Formatar CNPJ
    df["CNPJ_FORMATADO"] = df["_CNPJ_LIMPO"].apply(formatar_cnpj)

    # Criar valor formatado
    df["VALOR_CONSOLIDADO_BRL"] = df["VALOR_CONSOLIDADO_NUM"].apply(fmt_brl)

    return df

def gerar_relatorio(df, info_cnpjs, output_dir):
    """
    Gera relatório textual com resumo dos achados.
    """
    relatorio = []
    r = relatorio.append

    r("=" * 80)
    r("RELATÓRIO DE BUSCA EM CASCATA - FR9 E SF7 PARTICIPAÇÕES")
    r(f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    r("=" * 80)

    r("\n1. EMPRESAS ALVO")
    r("-" * 40)
    for emp in EMPRESAS_ALVO:
        r(f"  • {emp['cnpj']} - {emp['nome']}")

    r("\n2. RESULTADOS DA BUSCA")
    r("-" * 40)
    r(f"  Total de registros encontrados: {len(df)}")

    if not df.empty:
        # Inscrições únicas
        inscricoes_unicas = df["NUMERO_INSCRICAO"].nunique() if "NUMERO_INSCRICAO" in df.columns else 0
        r(f"  Inscrições únicas: {inscricoes_unicas}")

        # CNPJs únicos
        cnpjs_unicos = df["_CNPJ_LIMPO"].nunique() if "_CNPJ_LIMPO" in df.columns else 0
        r(f"  CNPJs únicos: {cnpjs_unicos}")

        # Valor total
        valor_total = df["VALOR_CONSOLIDADO_NUM"].sum()
        r(f"  Valor total consolidado: {fmt_brl(valor_total)}")

    r("\n3. CNPJS IDENTIFICADOS (ALVO + SOLIDÁRIOS)")
    r("-" * 40)

    # Ordenar por tipo
    alvos = [(k, v) for k, v in info_cnpjs.items() if v.get("origem") == "EMPRESA ALVO"]
    solidarios = [(k, v) for k, v in info_cnpjs.items() if v.get("origem") != "EMPRESA ALVO"]

    r("\n  EMPRESAS ALVO:")
    for cnpj, info in alvos:
        r(f"    • {formatar_cnpj(cnpj)} - {info['nome']}")

    if solidarios:
        r("\n  DEVEDORES SOLIDÁRIOS/VINCULADOS:")
        for cnpj, info in solidarios:
            tipo_dev = info.get("tipo", "N/A")
            r(f"    • {formatar_cnpj(cnpj)} - {info['nome']} [{tipo_dev}]")

    if not df.empty:
        r("\n4. DETALHAMENTO POR CNPJ")
        r("-" * 40)

        for cnpj_limpo in df["_CNPJ_LIMPO"].unique():
            df_cnpj = df[df["_CNPJ_LIMPO"] == cnpj_limpo]
            nome = df_cnpj["NOME_DEVEDOR"].iloc[0] if "NOME_DEVEDOR" in df_cnpj.columns else "N/A"
            qtd = len(df_cnpj)
            valor = df_cnpj["VALOR_CONSOLIDADO_NUM"].sum()

            r(f"\n  {formatar_cnpj(cnpj_limpo)} - {nome}")
            r(f"    Registros: {qtd}")
            r(f"    Valor: {fmt_brl(valor)}")

            # Por tipo de devedor
            if "TIPO_DEVEDOR" in df_cnpj.columns:
                tipos = df_cnpj.groupby("TIPO_DEVEDOR").agg({
                    "NUMERO_INSCRICAO": "count",
                    "VALOR_CONSOLIDADO_NUM": "sum"
                }).reset_index()
                for _, t in tipos.iterrows():
                    r(f"      - {t['TIPO_DEVEDOR']}: {t['NUMERO_INSCRICAO']} reg., {fmt_brl(t['VALOR_CONSOLIDADO_NUM'])}")

        r("\n5. RESUMO POR TIPO DE DEVEDOR")
        r("-" * 40)

        if "TIPO_DEVEDOR" in df.columns:
            resumo_tipo = df.groupby("TIPO_DEVEDOR").agg({
                "NUMERO_INSCRICAO": "count",
                "VALOR_CONSOLIDADO_NUM": "sum"
            }).reset_index()
            resumo_tipo = resumo_tipo.sort_values("VALOR_CONSOLIDADO_NUM", ascending=False)

            for _, row in resumo_tipo.iterrows():
                r(f"  {row['TIPO_DEVEDOR']}: {row['NUMERO_INSCRICAO']} registros - {fmt_brl(row['VALOR_CONSOLIDADO_NUM'])}")

        r("\n6. RESUMO POR REGIÃO")
        r("-" * 40)

        if "_REGIAO" in df.columns:
            resumo_regiao = df.groupby("_REGIAO").agg({
                "NUMERO_INSCRICAO": "count",
                "VALOR_CONSOLIDADO_NUM": "sum"
            }).reset_index()
            resumo_regiao = resumo_regiao.sort_values("VALOR_CONSOLIDADO_NUM", ascending=False)

            for _, row in resumo_regiao.iterrows():
                r(f"  {row['_REGIAO']}: {row['NUMERO_INSCRICAO']} registros - {fmt_brl(row['VALOR_CONSOLIDADO_NUM'])}")

    r("\n" + "=" * 80)
    r("FIM DO RELATÓRIO")
    r("=" * 80)

    # Salvar relatório
    caminho_relatorio = output_dir / "relatorio_busca_cascata.txt"
    with open(caminho_relatorio, "w", encoding="utf-8") as f:
        f.write("\n".join(relatorio))

    print(f"\n[OK] Relatório salvo: {caminho_relatorio}")

    # Imprimir na tela também
    print("\n".join(relatorio))

def gerar_grafico_pizza(df, output_dir):
    """
    Gera gráfico de pizza com fatia da dívida por CNPJ.
    """
    if df.empty:
        print("[!] Sem dados para gráfico de pizza.")
        return

    # Agrupar por CNPJ
    df_agrup = df.groupby(["_CNPJ_LIMPO", "NOME_DEVEDOR"]).agg({
        "VALOR_CONSOLIDADO_NUM": "sum"
    }).reset_index()

    df_agrup = df_agrup.sort_values("VALOR_CONSOLIDADO_NUM", ascending=False)

    # Se muitos CNPJs, agrupar os menores em "Outros"
    if len(df_agrup) > 10:
        top10 = df_agrup.head(10).copy()
        outros = df_agrup.iloc[10:]["VALOR_CONSOLIDADO_NUM"].sum()
        outros_df = pd.DataFrame([{
            "_CNPJ_LIMPO": "OUTROS",
            "NOME_DEVEDOR": f"Outros ({len(df_agrup) - 10} CNPJs)",
            "VALOR_CONSOLIDADO_NUM": outros
        }])
        df_agrup = pd.concat([top10, outros_df], ignore_index=True)

    # Preparar dados para o gráfico
    valores = df_agrup["VALOR_CONSOLIDADO_NUM"].values

    # Labels com CNPJ formatado + nome resumido
    labels = []
    for _, row in df_agrup.iterrows():
        cnpj = row["_CNPJ_LIMPO"]
        nome = row["NOME_DEVEDOR"][:25] + "..." if len(str(row["NOME_DEVEDOR"])) > 25 else row["NOME_DEVEDOR"]
        if cnpj == "OUTROS":
            labels.append(nome)
        else:
            labels.append(f"{formatar_cnpj(cnpj)}\n{nome}")

    # Cores
    cores = plt.cm.Set3(range(len(valores)))

    # Criar figura
    fig, ax = plt.subplots(figsize=(14, 10))

    # Destacar as empresas alvo
    explode = []
    cnpjs_alvo = [emp["cnpj_limpo"] for emp in EMPRESAS_ALVO]
    for _, row in df_agrup.iterrows():
        if row["_CNPJ_LIMPO"] in cnpjs_alvo:
            explode.append(0.05)
        else:
            explode.append(0)

    wedges, texts, autotexts = ax.pie(
        valores,
        labels=labels,
        autopct=lambda pct: f'{pct:.1f}%\n{fmt_brl(pct/100 * sum(valores))}',
        explode=explode,
        colors=cores,
        startangle=90,
        pctdistance=0.75
    )

    # Ajustar fonte
    for text in texts:
        text.set_fontsize(8)
    for autotext in autotexts:
        autotext.set_fontsize(7)

    ax.set_title(
        f"Distribuição da Dívida por CNPJ\n"
        f"FR9 e SF7 Participações + Solidários\n"
        f"Total: {fmt_brl(sum(valores))}",
        fontsize=12,
        fontweight="bold"
    )

    plt.tight_layout()

    caminho = output_dir / "pizza_divida_por_cnpj.png"
    plt.savefig(caminho, dpi=150, bbox_inches="tight")
    plt.close()

    print(f"[OK] Gráfico de pizza salvo: {caminho}")

def gerar_grafico_pizza_tipo_devedor(df, output_dir):
    """
    Gera gráfico de pizza por tipo de devedor.
    """
    if df.empty or "TIPO_DEVEDOR" not in df.columns:
        return

    df_agrup = df.groupby("TIPO_DEVEDOR").agg({
        "VALOR_CONSOLIDADO_NUM": "sum"
    }).reset_index()

    df_agrup = df_agrup.sort_values("VALOR_CONSOLIDADO_NUM", ascending=False)

    valores = df_agrup["VALOR_CONSOLIDADO_NUM"].values
    labels = df_agrup["TIPO_DEVEDOR"].values

    cores = plt.cm.Paired(range(len(valores)))

    fig, ax = plt.subplots(figsize=(10, 8))

    wedges, texts, autotexts = ax.pie(
        valores,
        labels=labels,
        autopct=lambda pct: f'{pct:.1f}%\n{fmt_brl(pct/100 * sum(valores))}',
        colors=cores,
        startangle=90,
        pctdistance=0.75
    )

    for autotext in autotexts:
        autotext.set_fontsize(9)

    ax.set_title(
        f"Distribuição da Dívida por Tipo de Devedor\n"
        f"Total: {fmt_brl(sum(valores))}",
        fontsize=12,
        fontweight="bold"
    )

    plt.tight_layout()

    caminho = output_dir / "pizza_divida_por_tipo_devedor.png"
    plt.savefig(caminho, dpi=150, bbox_inches="tight")
    plt.close()

    print(f"[OK] Gráfico por tipo de devedor salvo: {caminho}")

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 70)
    print("BUSCA EM CASCATA - FR9 E SF7 PARTICIPAÇÕES")
    print("=" * 70)
    print(f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"Diretório de dados: {DATA_DIR}")

    # Criar diretório de saída
    output_dir = DATA_DIR / "outputs_busca_fr9_sf7"
    output_dir.mkdir(exist_ok=True)
    print(f"Diretório de saída: {output_dir}")

    # Carregar todas as bases
    print("\n" + "=" * 70)
    print("CARREGANDO BASES PGFN")
    print("=" * 70)

    bases_carregadas = []
    for arquivo, regiao, _ in BASES:
        df = carregar_base(arquivo, regiao)
        if df is not None:
            bases_carregadas.append(df)
            print(f"    {regiao}: {len(df):,} registros")

    if not bases_carregadas:
        print("[ERRO] Nenhuma base foi carregada!")
        return

    print(f"\n  Total de bases carregadas: {len(bases_carregadas)}")
    total_registros = sum(len(df) for df in bases_carregadas)
    print(f"  Total de registros: {total_registros:,}")

    # Executar busca em cascata
    df_resultado, info_cnpjs = executar_busca_cascata(bases_carregadas)

    if df_resultado.empty:
        print("\n[!] Nenhum registro encontrado para as empresas alvo.")
        return

    # Processar resultados
    print("\n" + "=" * 70)
    print("PROCESSANDO RESULTADOS")
    print("=" * 70)

    df_resultado = processar_resultados(df_resultado, info_cnpjs)

    # Salvar CSV com todas as variáveis
    print("\n  Salvando CSV...")

    # Reordenar colunas para facilitar leitura
    colunas_primeiro = [
        "CNPJ_FORMATADO", "_CNPJ_LIMPO", "NOME_DEVEDOR", "TIPO_DEVEDOR",
        "NUMERO_INSCRICAO", "VALOR_CONSOLIDADO", "VALOR_CONSOLIDADO_NUM", "VALOR_CONSOLIDADO_BRL",
        "_CLASSIFICACAO", "_REGIAO"
    ]

    colunas_existentes = [c for c in colunas_primeiro if c in df_resultado.columns]
    outras_colunas = [c for c in df_resultado.columns if c not in colunas_existentes]
    colunas_ordenadas = colunas_existentes + outras_colunas

    df_resultado = df_resultado[colunas_ordenadas]

    caminho_csv = output_dir / "fr9_sf7_todas_inscricoes_cascata.csv"
    df_resultado.to_csv(caminho_csv, sep=";", index=False, encoding="utf-8-sig")
    print(f"  [OK] CSV salvo: {caminho_csv}")
    print(f"       {len(df_resultado):,} registros, {len(df_resultado.columns)} colunas")

    # Gerar relatório
    print("\n" + "=" * 70)
    print("GERANDO RELATÓRIO")
    print("=" * 70)

    gerar_relatorio(df_resultado, info_cnpjs, output_dir)

    # Gerar gráficos
    print("\n" + "=" * 70)
    print("GERANDO GRÁFICOS")
    print("=" * 70)

    gerar_grafico_pizza(df_resultado, output_dir)
    gerar_grafico_pizza_tipo_devedor(df_resultado, output_dir)

    print("\n" + "=" * 70)
    print("CONCLUÍDO!")
    print("=" * 70)
    print(f"\nArquivos gerados em: {output_dir}")
    print("  • fr9_sf7_todas_inscricoes_cascata.csv")
    print("  • relatorio_busca_cascata.txt")
    print("  • pizza_divida_por_cnpj.png")
    print("  • pizza_divida_por_tipo_devedor.png")

if __name__ == "__main__":
    main()
