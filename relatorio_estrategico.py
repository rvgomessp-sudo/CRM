"""
RELATORIO ESTRATEGICO - 5 PERGUNTAS COMERCIAIS
================================================
1. Quais tributos caracterizam empresa media/grande?
2. Como garantia se relaciona com tributo?
3. Como garantia se relaciona com safra?
4. Como n. de inscricoes por CNPJ muda o comportamento?
5. Onde esta a penhora substituivel (ouro comercial)?

Gera: relatorio_estrategico.txt + relatorio_estrategico.png + relatorio_penhora_ouro.csv
"""

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import sys
import io

import config
from utils import (
    fmt_brl, pipeline_base, extrair_garantia, classificar_safra,
    converter_valor_brl, salvar_csv, salvar_grafico, calcular_potencial_comercial
)

# Buffer para gravar relatorio em arquivo texto
_relatorio = io.StringIO()


def pr(texto="", end="\n"):
    """Imprime na tela e acumula no buffer do relatorio."""
    print(texto, end=end)
    _relatorio.write(texto + end)


def separador(titulo=""):
    pr("\n" + "=" * 100)
    if titulo:
        pr(f"  {titulo}")
        pr("=" * 100)


def carregar_todas_bases():
    """Carrega e concatena as 6 bases, somente PJ + tributos estruturais."""
    partes = []
    for arquivo, nome_curto, nome_completo in config.BASES:
        pr(f"  Lendo {nome_completo}...")
        try:
            df = pipeline_base(arquivo, colunas=config.COLUNAS_COMPLETAS)
        except FileNotFoundError as e:
            pr(f"    AVISO: {e}")
            continue
        df["REGIAO"] = nome_completo
        partes.append(df)
        pr(f"    OK: {len(df):,} registros")

    if not partes:
        pr("ERRO: Nenhuma base carregada. Verifique DATA_DIR no config.py")
        sys.exit(1)

    return pd.concat(partes, ignore_index=True)


# =====================================================================
# PERGUNTA 1 - Tributos que caracterizam empresa media/grande
# =====================================================================

def pergunta_1(df):
    separador("PERGUNTA 1: QUAIS TRIBUTOS CARACTERIZAM EMPRESA MEDIA/GRANDE?")

    pr("\nLogica: tributos estruturais (COFINS, PIS, IRPJ, CSLL, IPI, IOF, Imp. Importacao)")
    pr("excluindo ruido de pequeno porte (SIMPLES, MEI) e acessorios (MULTA, CUSTAS, IRRF).\n")

    # Normalizar nomes de tributo (pegar o tributo principal)
    df["TRIBUTO_NORM"] = df["RECEITA_PRINCIPAL"].astype(str).str.strip().str.upper()

    tributo_stats = df.groupby("TRIBUTO_NORM").agg(
        INSCRICOES=("NUMERO_INSCRICAO", "count"),
        CNPJs=("CPF_CNPJ", "nunique"),
        DIVIDA_TOTAL=("VALOR_CONSOLIDADO", "sum"),
        DIVIDA_MEDIA=("VALOR_CONSOLIDADO", "mean"),
        DIVIDA_MEDIANA=("VALOR_CONSOLIDADO", "median"),
        DIVIDA_P90=("VALOR_CONSOLIDADO", lambda x: x.quantile(0.90)),
    ).sort_values("DIVIDA_TOTAL", ascending=False)

    pr(f"{'TRIBUTO':<50} {'INSCR':>10} {'CNPJs':>8} {'DIVIDA TOTAL':>16} "
       f"{'MEDIA':>14} {'MEDIANA':>14} {'P90':>14}")
    pr("-" * 130)
    for tributo, row in tributo_stats.iterrows():
        nome = tributo[:48]
        pr(f"{nome:<50} {int(row['INSCRICOES']):>10,} {int(row['CNPJs']):>8,} "
           f"{fmt_brl(row['DIVIDA_TOTAL']):>16} {fmt_brl(row['DIVIDA_MEDIA']):>14} "
           f"{fmt_brl(row['DIVIDA_MEDIANA']):>14} {fmt_brl(row['DIVIDA_P90']):>14}")

    # Ticket medio por CNPJ por tributo
    pr("\n--- TICKET MEDIO POR CNPJ (divida total do CNPJ dentro do tributo) ---\n")
    cnpj_tributo = df.groupby(["CPF_CNPJ", "TRIBUTO_NORM"]).agg(
        DIVIDA_CNPJ=("VALOR_CONSOLIDADO", "sum"),
        QTD=("NUMERO_INSCRICAO", "count"),
    ).reset_index()

    ticket_por_tributo = cnpj_tributo.groupby("TRIBUTO_NORM").agg(
        CNPJs=("CPF_CNPJ", "nunique"),
        TICKET_MEDIO=("DIVIDA_CNPJ", "mean"),
        TICKET_MEDIANO=("DIVIDA_CNPJ", "median"),
        TICKET_P75=("DIVIDA_CNPJ", lambda x: x.quantile(0.75)),
        TICKET_P90=("DIVIDA_CNPJ", lambda x: x.quantile(0.90)),
        INSCR_MEDIA=("QTD", "mean"),
    ).sort_values("TICKET_MEDIO", ascending=False)

    pr(f"{'TRIBUTO':<50} {'CNPJs':>8} {'TICKET MEDIO':>16} {'MEDIANO':>14} "
       f"{'P75':>14} {'P90':>14} {'INSCR/CNPJ':>10}")
    pr("-" * 130)
    for tributo, row in ticket_por_tributo.iterrows():
        nome = tributo[:48]
        pr(f"{nome:<50} {int(row['CNPJs']):>8,} {fmt_brl(row['TICKET_MEDIO']):>16} "
           f"{fmt_brl(row['TICKET_MEDIANO']):>14} {fmt_brl(row['TICKET_P75']):>14} "
           f"{fmt_brl(row['TICKET_P90']):>14} {row['INSCR_MEDIA']:>10.1f}")

    pr("\n--- CONCLUSAO ---")
    pr("Tributos com ticket mediano mais alto indicam devedores de maior porte.")
    pr("IRPJ, CSLL e Imp. Importacao tipicamente apresentam tickets maiores (empresas grandes).")
    pr("COFINS e PIS tem volume maior mas ticket mais disperso (mistura medio + grande porte).")

    return tributo_stats


# =====================================================================
# PERGUNTA 2 - Garantia x Tributo
# =====================================================================

def pergunta_2(df):
    separador("PERGUNTA 2: COMO GARANTIA SE RELACIONA COM TRIBUTO?")

    # Somente ajuizados (garantia so faz sentido em ajuizamento)
    aj = df[df["AJUIZADO_BIN"]].copy()
    pr(f"\nBase: {len(aj):,} inscricoes ajuizadas | {aj['CPF_CNPJ'].nunique():,} CNPJs\n")

    # Tabela cruzada: Tributo x Garantia (contagem)
    cross_qtd = pd.crosstab(aj["TRIBUTO_NORM"], aj["GARANTIA_ATUAL"])
    # Percentual por linha
    cross_pct = pd.crosstab(aj["TRIBUTO_NORM"], aj["GARANTIA_ATUAL"], normalize="index") * 100

    # Valor medio por celula
    cross_valor = aj.groupby(["TRIBUTO_NORM", "GARANTIA_ATUAL"])["VALOR_CONSOLIDADO"].mean().unstack(fill_value=0)

    pr("--- DISTRIBUICAO % DE GARANTIA POR TRIBUTO (ajuizados) ---\n")
    garantias_order = ["SEM GARANTIA", "SEGURO GARANTIA", "PENHORA", "CARTA FIANCA", "DEPOSITO"]
    garantias_presentes = [g for g in garantias_order if g in cross_pct.columns]

    pr(f"{'TRIBUTO':<50} ", end="")
    for g in garantias_presentes:
        pr(f"{g[:15]:>16} ", end="")
    pr(f"{'TOTAL INSCR':>12}")
    pr("-" * (50 + 16 * len(garantias_presentes) + 14))

    for tributo in cross_pct.index:
        nome = tributo[:48]
        pr(f"{nome:<50} ", end="")
        for g in garantias_presentes:
            val = cross_pct.loc[tributo, g] if g in cross_pct.columns else 0
            pr(f"{val:>15.1f}% ", end="")
        total = cross_qtd.loc[tributo].sum()
        pr(f"{total:>12,}")

    # Resumo: quais tributos tem mais seguro garantia / penhora
    pr("\n--- TRIBUTOS COM MAIOR PENETRACAO DE SEGURO GARANTIA ---\n")
    if "SEGURO GARANTIA" in cross_pct.columns:
        sg_rank = cross_pct["SEGURO GARANTIA"].sort_values(ascending=False)
        for i, (tributo, pct) in enumerate(sg_rank.items(), 1):
            qtd = cross_qtd.loc[tributo, "SEGURO GARANTIA"] if "SEGURO GARANTIA" in cross_qtd.columns else 0
            pr(f"  {i}. {tributo[:55]:<55} {pct:.1f}% ({qtd:,} inscricoes)")
            if i >= 10:
                break

    pr("\n--- TRIBUTOS COM MAIOR PENETRACAO DE PENHORA ---\n")
    if "PENHORA" in cross_pct.columns:
        pen_rank = cross_pct["PENHORA"].sort_values(ascending=False)
        for i, (tributo, pct) in enumerate(pen_rank.items(), 1):
            qtd = cross_qtd.loc[tributo, "PENHORA"] if "PENHORA" in cross_qtd.columns else 0
            pr(f"  {i}. {tributo[:55]:<55} {pct:.1f}% ({qtd:,} inscricoes)")
            if i >= 10:
                break

    pr("\n--- CONCLUSAO ---")
    pr("Tributos com alta % de 'SEM GARANTIA' sao mercado virgem para seguro garantia.")
    pr("Tributos com alta % de 'PENHORA' sao mercado de substituicao (convencer a trocar).")

    return cross_pct, cross_qtd


# =====================================================================
# PERGUNTA 3 - Garantia x Safra
# =====================================================================

def pergunta_3(df):
    separador("PERGUNTA 3: COMO GARANTIA SE RELACIONA COM SAFRA?")

    aj = df[df["AJUIZADO_BIN"]].copy()
    pr(f"\nBase: {len(aj):,} inscricoes ajuizadas\n")

    safras_order = ["2024-2025", "2023", "2022", "2021", "2020", "2019", "<=2018"]
    garantias_order = ["SEM GARANTIA", "SEGURO GARANTIA", "PENHORA", "CARTA FIANCA", "DEPOSITO"]

    # Contagem e percentual
    cross_qtd = pd.crosstab(aj["SAFRA"], aj["GARANTIA_ATUAL"])
    cross_pct = pd.crosstab(aj["SAFRA"], aj["GARANTIA_ATUAL"], normalize="index") * 100

    safras_presentes = [s for s in safras_order if s in cross_pct.index]
    garantias_presentes = [g for g in garantias_order if g in cross_pct.columns]

    pr("--- DISTRIBUICAO % DE GARANTIA POR SAFRA ---\n")
    pr(f"{'SAFRA':<12} ", end="")
    for g in garantias_presentes:
        pr(f"{g[:15]:>16} ", end="")
    pr(f"{'TOTAL':>12}")
    pr("-" * (12 + 16 * len(garantias_presentes) + 14))

    for safra in safras_presentes:
        pr(f"{safra:<12} ", end="")
        for g in garantias_presentes:
            val = cross_pct.loc[safra, g] if g in cross_pct.columns else 0
            pr(f"{val:>15.1f}% ", end="")
        total = cross_qtd.loc[safra].sum()
        pr(f"{total:>12,}")

    # Valor medio por safra x garantia
    pr("\n--- VALOR MEDIO POR SAFRA x GARANTIA ---\n")
    cross_media = aj.groupby(["SAFRA", "GARANTIA_ATUAL"])["VALOR_CONSOLIDADO"].mean().unstack(fill_value=0)

    pr(f"{'SAFRA':<12} ", end="")
    for g in garantias_presentes:
        pr(f"{g[:15]:>16} ", end="")
    pr()
    pr("-" * (12 + 16 * len(garantias_presentes) + 2))

    for safra in safras_presentes:
        if safra in cross_media.index:
            pr(f"{safra:<12} ", end="")
            for g in garantias_presentes:
                val = cross_media.loc[safra, g] if g in cross_media.columns else 0
                pr(f"{fmt_brl(val):>16} ", end="")
            pr()

    pr("\n--- EVOLUCAO: % SEM GARANTIA AO LONGO DO TEMPO ---\n")
    if "SEM GARANTIA" in cross_pct.columns:
        for safra in safras_presentes:
            pct = cross_pct.loc[safra, "SEM GARANTIA"]
            qtd = cross_qtd.loc[safra, "SEM GARANTIA"] if "SEM GARANTIA" in cross_qtd.columns else 0
            barra = "#" * int(pct / 2)
            pr(f"  {safra:<12} {pct:>5.1f}% | {barra} ({qtd:,})")

    pr("\n--- CONCLUSAO ---")
    pr("Safras mais recentes (2024-2025, 2023) tendem a ter MAIOR % sem garantia -> mercado novo.")
    pr("Safras antigas (<=2018, 2019, 2020) com penhora -> candidatas a substituicao.")
    pr("A safra ideal para prospecao e aquela com alto volume + alta % sem garantia.")

    return cross_pct


# =====================================================================
# PERGUNTA 4 - Numero de inscricoes por CNPJ e comportamento
# =====================================================================

def pergunta_4(df):
    separador("PERGUNTA 4: COMO N. DE INSCRICOES POR CNPJ MUDA O COMPORTAMENTO?")

    aj = df[df["AJUIZADO_BIN"]].copy()

    # Agregar por CNPJ
    cnpj = aj.groupby("CPF_CNPJ").agg(
        NOME=("NOME_DEVEDOR", "first"),
        UF=("UF_DEVEDOR", "first"),
        QTD_INSCRICOES=("NUMERO_INSCRICAO", "count"),
        DIVIDA_TOTAL=("VALOR_CONSOLIDADO", "sum"),
        DIVIDA_MEDIA=("VALOR_CONSOLIDADO", "mean"),
        DIVIDA_MAX=("VALOR_CONSOLIDADO", "max"),
        TEM_SEGURO=("GARANTIA_ATUAL", lambda x: (x == "SEGURO GARANTIA").any()),
        TEM_PENHORA=("GARANTIA_ATUAL", lambda x: (x == "PENHORA").any()),
        TEM_SEM_GAR=("GARANTIA_ATUAL", lambda x: (x == "SEM GARANTIA").any()),
        PCT_SEM_GAR=("GARANTIA_ATUAL", lambda x: (x == "SEM GARANTIA").sum() / len(x) * 100),
        QTD_SAFRAS=("SAFRA", "nunique"),
        REGIAO=("REGIAO", "first"),
    ).reset_index()

    # Faixas de inscricoes
    faixas_inscr = [
        ("1 inscricao", 1, 1),
        ("2-3 inscricoes", 2, 3),
        ("4-5 inscricoes", 4, 5),
        ("6-10 inscricoes", 6, 10),
        ("11-20 inscricoes", 11, 20),
        ("21-50 inscricoes", 21, 50),
        ("51-100 inscricoes", 51, 100),
        ("101+ inscricoes", 101, 999_999),
    ]

    pr(f"\nBase: {len(cnpj):,} CNPJs ajuizados\n")

    pr(f"{'FAIXA INSCR':<20} {'CNPJs':>8} {'%':>7} {'DIV MEDIA':>16} {'DIV MEDIANA':>16} "
       f"{'%SEM GAR':>10} {'%TEM SG':>10} {'%TEM PENH':>10}")
    pr("-" * 105)

    resultados_faixas = []
    for nome, vmin, vmax in faixas_inscr:
        subset = cnpj[(cnpj["QTD_INSCRICOES"] >= vmin) & (cnpj["QTD_INSCRICOES"] <= vmax)]
        if len(subset) == 0:
            continue
        pct_total = len(subset) / len(cnpj) * 100
        div_media = subset["DIVIDA_TOTAL"].mean()
        div_mediana = subset["DIVIDA_TOTAL"].median()
        pct_sem = subset["PCT_SEM_GAR"].mean()
        pct_sg = subset["TEM_SEGURO"].sum() / len(subset) * 100
        pct_penh = subset["TEM_PENHORA"].sum() / len(subset) * 100

        pr(f"{nome:<20} {len(subset):>8,} {pct_total:>6.1f}% {fmt_brl(div_media):>16} "
           f"{fmt_brl(div_mediana):>16} {pct_sem:>9.1f}% {pct_sg:>9.1f}% {pct_penh:>9.1f}%")

        resultados_faixas.append({
            "faixa": nome, "cnpjs": len(subset), "div_media": div_media,
            "pct_sem_gar": pct_sem, "pct_seguro": pct_sg, "pct_penhora": pct_penh
        })

    # Correlacao inscricoes x divida
    pr(f"\n--- CORRELACAO ---")
    corr = cnpj["QTD_INSCRICOES"].corr(cnpj["DIVIDA_TOTAL"])
    pr(f"  Correlacao (n. inscricoes x divida total): {corr:.4f}")
    corr_log = np.log1p(cnpj["QTD_INSCRICOES"]).corr(np.log1p(cnpj["DIVIDA_TOTAL"]))
    pr(f"  Correlacao log-log: {corr_log:.4f}")

    # CNPJs com muitas inscricoes: comportamento diferenciado
    pr(f"\n--- CNPJs COM 10+ INSCRICOES (DEVEDORES RECORRENTES) ---")
    recorrentes = cnpj[cnpj["QTD_INSCRICOES"] >= 10]
    pr(f"  Quantidade: {len(recorrentes):,} CNPJs ({len(recorrentes)/len(cnpj)*100:.1f}%)")
    pr(f"  Divida total: {fmt_brl(recorrentes['DIVIDA_TOTAL'].sum())}")
    pr(f"  % da divida total: {recorrentes['DIVIDA_TOTAL'].sum() / cnpj['DIVIDA_TOTAL'].sum() * 100:.1f}%")
    pr(f"  % com seguro garantia: {recorrentes['TEM_SEGURO'].sum() / len(recorrentes) * 100:.1f}%")
    pr(f"  % com penhora: {recorrentes['TEM_PENHORA'].sum() / len(recorrentes) * 100:.1f}%")
    pr(f"  Ticket medio: {fmt_brl(recorrentes['DIVIDA_TOTAL'].mean())}")

    pr("\n--- CONCLUSAO ---")
    pr("CNPJs com mais inscricoes = devedores maiores e recorrentes.")
    pr("Concentracao: poucos CNPJs com 10+ inscricoes detêm grande parte da divida.")
    pr("Quanto mais inscricoes, MAIOR a probabilidade de ja ter alguma garantia (inclusive penhora).")
    pr("Alvo ideal: 4-20 inscricoes -> ticket relevante + ainda alta % sem garantia.")

    return cnpj, resultados_faixas


# =====================================================================
# PERGUNTA 5 - Penhora substituivel (ouro comercial)
# =====================================================================

def pergunta_5(df):
    separador("PERGUNTA 5: ONDE ESTA A PENHORA SUBSTITUIVEL (OURO COMERCIAL)?")

    pr("\nLogica: inscricoes com PENHORA sao candidatas a substituicao por Seguro Garantia.")
    pr("A empresa ja tem processo ajuizado e garantia constituida -> trocar penhora por SG")
    pr("libera o ativo penhorado e pode ser mais vantajoso para a empresa.\n")

    aj = df[df["AJUIZADO_BIN"]].copy()
    penhora = aj[aj["GARANTIA_ATUAL"] == "PENHORA"].copy()

    total_aj = len(aj)
    total_penh = len(penhora)
    pr(f"  Inscricoes ajuizadas: {total_aj:,}")
    pr(f"  Inscricoes com PENHORA: {total_penh:,} ({total_penh/total_aj*100:.1f}%)")
    pr(f"  CNPJs com PENHORA: {penhora['CPF_CNPJ'].nunique():,}")
    pr(f"  Divida total com PENHORA: {fmt_brl(penhora['VALOR_CONSOLIDADO'].sum())}")

    # 5a. Penhora por tributo
    pr("\n--- PENHORA POR TRIBUTO ---\n")
    penh_tributo = penhora.groupby("TRIBUTO_NORM").agg(
        INSCRICOES=("NUMERO_INSCRICAO", "count"),
        CNPJs=("CPF_CNPJ", "nunique"),
        DIVIDA=("VALOR_CONSOLIDADO", "sum"),
        MEDIA=("VALOR_CONSOLIDADO", "mean"),
    ).sort_values("DIVIDA", ascending=False)

    pr(f"{'TRIBUTO':<50} {'INSCR':>10} {'CNPJs':>8} {'DIVIDA':>18} {'MEDIA':>14}")
    pr("-" * 105)
    for tributo, row in penh_tributo.iterrows():
        nome = tributo[:48]
        pr(f"{nome:<50} {int(row['INSCRICOES']):>10,} {int(row['CNPJs']):>8,} "
           f"{fmt_brl(row['DIVIDA']):>18} {fmt_brl(row['MEDIA']):>14}")

    # 5b. Penhora por safra
    pr("\n--- PENHORA POR SAFRA ---\n")
    safras_order = ["2024-2025", "2023", "2022", "2021", "2020", "2019", "<=2018"]
    penh_safra = penhora.groupby("SAFRA").agg(
        INSCRICOES=("NUMERO_INSCRICAO", "count"),
        CNPJs=("CPF_CNPJ", "nunique"),
        DIVIDA=("VALOR_CONSOLIDADO", "sum"),
        MEDIA=("VALOR_CONSOLIDADO", "mean"),
    )

    pr(f"{'SAFRA':<12} {'INSCR':>10} {'CNPJs':>8} {'DIVIDA':>18} {'MEDIA':>14}")
    pr("-" * 65)
    for safra in safras_order:
        if safra in penh_safra.index:
            row = penh_safra.loc[safra]
            pr(f"{safra:<12} {int(row['INSCRICOES']):>10,} {int(row['CNPJs']):>8,} "
               f"{fmt_brl(row['DIVIDA']):>18} {fmt_brl(row['MEDIA']):>14}")

    # 5c. Penhora por regiao
    pr("\n--- PENHORA POR REGIAO ---\n")
    penh_regiao = penhora.groupby("REGIAO").agg(
        INSCRICOES=("NUMERO_INSCRICAO", "count"),
        CNPJs=("CPF_CNPJ", "nunique"),
        DIVIDA=("VALOR_CONSOLIDADO", "sum"),
    ).sort_values("DIVIDA", ascending=False)

    pr(f"{'REGIAO':<40} {'INSCR':>10} {'CNPJs':>8} {'DIVIDA':>18}")
    pr("-" * 80)
    for regiao, row in penh_regiao.iterrows():
        pr(f"{regiao:<40} {int(row['INSCRICOES']):>10,} {int(row['CNPJs']):>8,} "
           f"{fmt_brl(row['DIVIDA']):>18}")

    # 5d. Penhora por faixa de valor (ticket)
    pr("\n--- PENHORA POR FAIXA DE VALOR ---\n")
    pr(f"{'FAIXA':<25} {'INSCR':>10} {'CNPJs':>8} {'DIVIDA':>18} {'PREMIO 2%':>16} {'COMISSAO 25%':>16}")
    pr("-" * 100)
    for nome, vmin, vmax in config.FAIXAS_TICKET:
        subset = penhora[
            (penhora["VALOR_CONSOLIDADO"] >= vmin) &
            (penhora["VALOR_CONSOLIDADO"] < vmax)
        ]
        if len(subset) == 0:
            continue
        divida = subset["VALOR_CONSOLIDADO"].sum()
        premio, comissao = calcular_potencial_comercial(divida)
        pr(f"{nome:<25} {len(subset):>10,} {subset['CPF_CNPJ'].nunique():>8,} "
           f"{fmt_brl(divida):>18} {fmt_brl(premio):>16} {fmt_brl(comissao):>16}")

    # 5e. OURO: CNPJs com penhora >= R$ 1mi (substituicao viavel)
    pr("\n--- OURO COMERCIAL: CNPJs COM PENHORA >= R$ 1 MI ---\n")
    penh_cnpj = penhora.groupby("CPF_CNPJ").agg(
        NOME=("NOME_DEVEDOR", "first"),
        UF=("UF_DEVEDOR", "first"),
        QTD_INSCRICOES=("NUMERO_INSCRICAO", "count"),
        DIVIDA_TOTAL=("VALOR_CONSOLIDADO", "sum"),
        DIVIDA_MAX=("VALOR_CONSOLIDADO", "max"),
        SAFRA_MAIS_ANTIGA=("SAFRA", "first"),
        REGIAO=("REGIAO", "first"),
    ).reset_index()

    ouro = penh_cnpj[penh_cnpj["DIVIDA_TOTAL"] >= config.TICKET_MINIMO].sort_values(
        "DIVIDA_TOTAL", ascending=False
    )

    pr(f"  CNPJs com penhora >= R$ 1 mi: {len(ouro):,}")
    pr(f"  Divida total destes CNPJs: {fmt_brl(ouro['DIVIDA_TOTAL'].sum())}")
    premio_ouro, comissao_ouro = calcular_potencial_comercial(ouro["DIVIDA_TOTAL"].sum())
    pr(f"  Premio potencial (2%): {fmt_brl(premio_ouro)}")
    pr(f"  Comissao potencial (25%): {fmt_brl(comissao_ouro)}")

    # Top 30
    pr(f"\n  TOP 30 CNPJs - PENHORA SUBSTITUIVEL:\n")
    pr(f"  {'#':>3} {'CNPJ':<20} {'DIVIDA TOTAL':>18} {'INSCR':>6} {'UF':>4} {'REGIAO':<35}")
    pr("  " + "-" * 90)
    for i, (_, row) in enumerate(ouro.head(30).iterrows(), 1):
        pr(f"  {i:>3} {row['CPF_CNPJ']:<20} {fmt_brl(row['DIVIDA_TOTAL']):>18} "
           f"{row['QTD_INSCRICOES']:>6} {str(row['UF']):>4} {str(row['REGIAO'])[:33]:<35}")

    # 5f. Penhora + Sem Garantia no mesmo CNPJ (cross-sell)
    pr("\n--- CROSS-SELL: CNPJs COM PENHORA *E* SEM GARANTIA ---\n")
    cnpjs_penhora = set(penhora["CPF_CNPJ"].unique())
    sem_gar = aj[aj["GARANTIA_ATUAL"] == "SEM GARANTIA"]
    cnpjs_sem = set(sem_gar["CPF_CNPJ"].unique())
    cnpjs_cross = cnpjs_penhora & cnpjs_sem

    pr(f"  CNPJs so com penhora: {len(cnpjs_penhora - cnpjs_sem):,}")
    pr(f"  CNPJs so sem garantia: {len(cnpjs_sem - cnpjs_penhora):,}")
    pr(f"  CNPJs com AMBOS (cross-sell): {len(cnpjs_cross):,}")

    if cnpjs_cross:
        cross_df = aj[aj["CPF_CNPJ"].isin(cnpjs_cross)].copy()
        cross_agg = cross_df.groupby("CPF_CNPJ").agg(
            DIVIDA_TOTAL=("VALOR_CONSOLIDADO", "sum"),
            QTD_TOTAL=("NUMERO_INSCRICAO", "count"),
            QTD_PENHORA=("GARANTIA_ATUAL", lambda x: (x == "PENHORA").sum()),
            QTD_SEM_GAR=("GARANTIA_ATUAL", lambda x: (x == "SEM GARANTIA").sum()),
        ).reset_index().sort_values("DIVIDA_TOTAL", ascending=False)

        # Divida especifica das penhoras dentro do cross-sell
        div_penh_cross = cross_df[cross_df["GARANTIA_ATUAL"] == "PENHORA"]["VALOR_CONSOLIDADO"].sum()
        div_sem_cross = cross_df[cross_df["GARANTIA_ATUAL"] == "SEM GARANTIA"]["VALOR_CONSOLIDADO"].sum()
        pr(f"  Divida total cross-sell: {fmt_brl(cross_agg['DIVIDA_TOTAL'].sum())}")
        pr(f"    -> Penhora (substituivel): {fmt_brl(div_penh_cross)}")
        pr(f"    -> Sem garantia (venda nova): {fmt_brl(div_sem_cross)}")

    pr("\n--- CONCLUSAO ---")
    pr("PENHORA SUBSTITUIVEL e o 'ouro' comercial:")
    pr("  1. Empresa ja tem garantia constituida (prova de capacidade financeira)")
    pr("  2. Substituir penhora por SG libera ativos -> argumento de venda forte")
    pr("  3. Foco: CNPJs com penhora >= R$ 1 mi e inscricoes recentes (2020-2025)")
    pr("  4. Cross-sell: CNPJs que tem penhora E inscricoes sem garantia = dupla oportunidade")

    return ouro


# =====================================================================
# GRAFICOS
# =====================================================================

def gerar_graficos(df, tributo_stats, cross_gar_safra, cnpj_dados, resultados_faixas, ouro):
    """Gera painel de 8 graficos com as respostas visuais."""
    fig, axes = plt.subplots(3, 3, figsize=(22, 18))

    aj = df[df["AJUIZADO_BIN"]].copy()

    # 1. Top tributos por divida
    ax = axes[0, 0]
    top_trib = tributo_stats.head(8)
    bars = ax.barh(range(len(top_trib)), top_trib["DIVIDA_TOTAL"] / 1e12,
                   color='#3498db', edgecolor='white')
    ax.set_yticks(range(len(top_trib)))
    ax.set_yticklabels([t[:30] for t in top_trib.index], fontsize=8)
    ax.set_xlabel("R$ trilhoes")
    ax.set_title("P1: Top Tributos por Divida Total", fontweight="bold", fontsize=10)
    ax.invert_yaxis()

    # 2. Garantia x Tributo (heatmap simplificado)
    ax = axes[0, 1]
    garantias_plot = ["SEM GARANTIA", "PENHORA", "SEGURO GARANTIA"]
    top_tribs = tributo_stats.head(6).index.tolist()
    cross_heat = pd.crosstab(aj["TRIBUTO_NORM"], aj["GARANTIA_ATUAL"], normalize="index") * 100
    data_heat = cross_heat.reindex(index=top_tribs, columns=garantias_plot).fillna(0)
    im = ax.imshow(data_heat.values, cmap="YlOrRd", aspect="auto")
    ax.set_xticks(range(len(garantias_plot)))
    ax.set_xticklabels([g[:12] for g in garantias_plot], fontsize=8, rotation=45, ha="right")
    ax.set_yticks(range(len(top_tribs)))
    ax.set_yticklabels([t[:25] for t in top_tribs], fontsize=8)
    for i in range(len(top_tribs)):
        for j in range(len(garantias_plot)):
            ax.text(j, i, f"{data_heat.values[i, j]:.0f}%",
                    ha="center", va="center", fontsize=8, fontweight="bold")
    ax.set_title("P2: % Garantia por Tributo (top 6)", fontweight="bold", fontsize=10)

    # 3. Garantia x Safra
    ax = axes[0, 2]
    safras_order = ["2024-2025", "2023", "2022", "2021", "2020", "2019", "<=2018"]
    safras_presentes = [s for s in safras_order if s in cross_gar_safra.index]
    cols_plot = [c for c in ["SEM GARANTIA", "SEGURO GARANTIA", "PENHORA"] if c in cross_gar_safra.columns]
    cores_gar = {"SEM GARANTIA": "#95a5a6", "SEGURO GARANTIA": "#27ae60", "PENHORA": "#e74c3c"}
    bottom = np.zeros(len(safras_presentes))
    for g in cols_plot:
        vals = [cross_gar_safra.loc[s, g] if s in cross_gar_safra.index else 0 for s in safras_presentes]
        ax.bar(safras_presentes, vals, bottom=bottom, label=g[:12],
               color=cores_gar.get(g, "#bdc3c7"))
        bottom += np.array(vals)
    ax.set_title("P3: % Garantia por Safra", fontweight="bold", fontsize=10)
    ax.set_ylabel("%")
    ax.legend(fontsize=7, loc="upper right")
    ax.tick_params(axis="x", rotation=45)

    # 4. N. inscricoes x Divida media
    ax = axes[1, 0]
    if resultados_faixas:
        faixas_df = pd.DataFrame(resultados_faixas)
        ax.bar(range(len(faixas_df)), faixas_df["div_media"] / 1e6, color="#2ecc71")
        ax.set_xticks(range(len(faixas_df)))
        ax.set_xticklabels(faixas_df["faixa"], fontsize=7, rotation=45, ha="right")
        ax.set_ylabel("R$ milhoes")
        ax.set_title("P4: Divida Media por Faixa de Inscricoes", fontweight="bold", fontsize=10)

    # 5. N. inscricoes x % sem garantia
    ax = axes[1, 1]
    if resultados_faixas:
        ax.bar(range(len(faixas_df)), faixas_df["pct_sem_gar"], color="#e67e22", label="% Sem Gar")
        ax.bar(range(len(faixas_df)), faixas_df["pct_seguro"], color="#27ae60",
               alpha=0.7, label="% Seguro")
        ax.bar(range(len(faixas_df)), faixas_df["pct_penhora"], color="#e74c3c",
               alpha=0.7, label="% Penhora")
        ax.set_xticks(range(len(faixas_df)))
        ax.set_xticklabels(faixas_df["faixa"], fontsize=7, rotation=45, ha="right")
        ax.set_ylabel("%")
        ax.set_title("P4: % Garantia por Faixa de Inscricoes", fontweight="bold", fontsize=10)
        ax.legend(fontsize=7)

    # 6. Penhora por regiao
    ax = axes[1, 2]
    penhora = aj[aj["GARANTIA_ATUAL"] == "PENHORA"]
    penh_reg = penhora.groupby("REGIAO")["VALOR_CONSOLIDADO"].sum().sort_values(ascending=True)
    ax.barh(range(len(penh_reg)), penh_reg.values / 1e9, color="#e74c3c")
    ax.set_yticks(range(len(penh_reg)))
    ax.set_yticklabels([r[:30] for r in penh_reg.index], fontsize=8)
    ax.set_xlabel("R$ bilhoes")
    ax.set_title("P5: Penhora por Regiao (R$ bi)", fontweight="bold", fontsize=10)

    # 7. Penhora por faixa de valor
    ax = axes[2, 0]
    faixas_penh = []
    for nome, vmin, vmax in config.FAIXAS_TICKET:
        s = penhora[(penhora["VALOR_CONSOLIDADO"] >= vmin) & (penhora["VALOR_CONSOLIDADO"] < vmax)]
        if len(s) > 0:
            faixas_penh.append({"faixa": nome, "divida": s["VALOR_CONSOLIDADO"].sum()})
    if faixas_penh:
        fp = pd.DataFrame(faixas_penh)
        ax.bar(range(len(fp)), fp["divida"] / 1e9, color="#c0392b")
        ax.set_xticks(range(len(fp)))
        ax.set_xticklabels(fp["faixa"], fontsize=7, rotation=45, ha="right")
        ax.set_ylabel("R$ bilhoes")
        ax.set_title("P5: Penhora por Faixa de Valor", fontweight="bold", fontsize=10)

    # 8. Top 15 CNPJs ouro (penhora substituivel)
    ax = axes[2, 1]
    if len(ouro) > 0:
        top15 = ouro.head(15).sort_values("DIVIDA_TOTAL")
        ax.barh(range(len(top15)), top15["DIVIDA_TOTAL"] / 1e6, color="#f39c12")
        ax.set_yticks(range(len(top15)))
        ax.set_yticklabels([f"...{c[-6:]}" for c in top15["CPF_CNPJ"]], fontsize=8)
        ax.set_xlabel("R$ milhoes")
        ax.set_title("P5: Top 15 CNPJs - Penhora Substituivel", fontweight="bold", fontsize=10)

    # 9. Resumo geral - mercado enderecavel
    ax = axes[2, 2]
    ax.axis("off")
    sem_gar = aj[aj["GARANTIA_ATUAL"] == "SEM GARANTIA"]
    penh_total = penhora["VALOR_CONSOLIDADO"].sum()
    sem_total = sem_gar["VALOR_CONSOLIDADO"].sum()
    ouro_total = ouro["DIVIDA_TOTAL"].sum() if len(ouro) > 0 else 0

    texto_resumo = (
        f"MERCADO ENDERECAVEL\n"
        f"{'='*35}\n\n"
        f"SEM GARANTIA\n"
        f"  Inscricoes: {len(sem_gar):,}\n"
        f"  CNPJs: {sem_gar['CPF_CNPJ'].nunique():,}\n"
        f"  Divida: {fmt_brl(sem_total)}\n\n"
        f"PENHORA (substituivel)\n"
        f"  Inscricoes: {len(penhora):,}\n"
        f"  CNPJs: {penhora['CPF_CNPJ'].nunique():,}\n"
        f"  Divida: {fmt_brl(penh_total)}\n\n"
        f"OURO (penhora >= R$1mi)\n"
        f"  CNPJs: {len(ouro):,}\n"
        f"  Divida: {fmt_brl(ouro_total)}\n"
        f"  Premio: {fmt_brl(ouro_total * 0.02)}\n"
        f"  Comissao: {fmt_brl(ouro_total * 0.005)}\n"
    )
    ax.text(0.05, 0.95, texto_resumo, transform=ax.transAxes,
            fontsize=9, verticalalignment="top", fontfamily="monospace",
            bbox=dict(boxstyle="round", facecolor="#ecf0f1", alpha=0.8))

    plt.suptitle("RELATORIO ESTRATEGICO - 5 PERGUNTAS COMERCIAIS",
                 fontsize=14, fontweight="bold", y=1.01)
    plt.tight_layout()
    return fig


# =====================================================================
# MAIN
# =====================================================================

def main():
    separador("RELATORIO ESTRATEGICO - CRM PGFN")
    pr(f"  Data: {pd.Timestamp.today().strftime('%Y-%m-%d %H:%M')}")
    pr(f"  Diretorio de dados: {config.DATA_DIR}\n")

    # Carregar dados
    df = carregar_todas_bases()
    df["TRIBUTO_NORM"] = df["RECEITA_PRINCIPAL"].astype(str).str.strip().str.upper()

    pr(f"\n  DADOS CARREGADOS:")
    pr(f"    Total inscricoes: {len(df):,}")
    pr(f"    CNPJs unicos: {df['CPF_CNPJ'].nunique():,}")
    pr(f"    Divida total: {fmt_brl(df['VALOR_CONSOLIDADO'].sum())}")
    aj = df[df["AJUIZADO_BIN"]]
    pr(f"    Ajuizados: {len(aj):,} ({len(aj)/len(df)*100:.1f}%)")

    # Executar as 5 analises
    tributo_stats = pergunta_1(df)
    cross_gar_trib, cross_gar_trib_qtd = pergunta_2(df)
    cross_gar_safra = pergunta_3(df)
    cnpj_dados, resultados_faixas = pergunta_4(df)
    ouro = pergunta_5(df)

    # Graficos
    separador("GERANDO GRAFICOS")
    fig = gerar_graficos(df, tributo_stats, cross_gar_safra, cnpj_dados, resultados_faixas, ouro)
    salvar_grafico(fig, "relatorio_estrategico.png")
    plt.close(fig)

    # Exportar CSV do ouro
    salvar_csv(ouro, "relatorio_penhora_ouro.csv")

    # Salvar relatorio texto
    relatorio_path = config.get_output_path("relatorio_estrategico.txt")
    with open(relatorio_path, "w", encoding="utf-8") as f:
        f.write(_relatorio.getvalue())
    pr(f"\n  Relatorio texto salvo: {relatorio_path}")

    separador("RELATORIO CONCLUIDO")


if __name__ == "__main__":
    main()
