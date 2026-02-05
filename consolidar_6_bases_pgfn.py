"""
Consolidacao das 6 bases regionais PGFN - Brasil completo.

Processa cada base individualmente, extrai metricas por tipo de garantia
e gera um painel consolidado com graficos e CSV de resumo.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys

import config
from utils import (
    fmt_brl, extrair_garantia, pipeline_base, salvar_csv, salvar_grafico
)


def processar_base(i, arquivo, nome_regiao):
    """Processa uma base regional e retorna dicionario de metricas."""
    print(f"\n{'='*80}")
    print(f"  BASE {i}: {arquivo} ({nome_regiao})")
    print(f"{'='*80}")

    df = pipeline_base(arquivo, colunas=config.COLUNAS_COMPLETAS)
    total_filtrado = len(df)

    cnpjs_total = df["CPF_CNPJ"].nunique()
    divida_total = df["VALOR_CONSOLIDADO"].sum()

    # Ajuizados
    ajuizados = df[df["AJUIZADO_BIN"]].copy()
    ajuizados_qtd = len(ajuizados)
    ajuizados_cnpjs = ajuizados["CPF_CNPJ"].nunique()
    ajuizados_divida = ajuizados["VALOR_CONSOLIDADO"].sum()

    # Por tipo de garantia (ajuizados)
    garantias_metricas = {}
    for gar in ["SEGURO GARANTIA", "CARTA FIANCA", "DEPOSITO", "PENHORA", "SEM GARANTIA"]:
        subset = ajuizados[ajuizados["GARANTIA_ATUAL"] == gar]
        garantias_metricas[gar] = {
            "qtd": len(subset),
            "cnpjs": subset["CPF_CNPJ"].nunique(),
            "divida": subset["VALOR_CONSOLIDADO"].sum(),
        }

    # Medias ajuizados sem garantia
    sem_df = ajuizados[ajuizados["GARANTIA_ATUAL"] == "SEM GARANTIA"]
    sem_media = sem_df["VALOR_CONSOLIDADO"].mean() if len(sem_df) > 0 else 0
    sem_mediana = sem_df["VALOR_CONSOLIDADO"].median() if len(sem_df) > 0 else 0

    resultado = {
        "BASE": f"SIDA_{i}",
        "TOTAL_FILTRADO": total_filtrado,
        "CNPJS_TOTAL": cnpjs_total,
        "DIVIDA_TOTAL": divida_total,
        "AJUIZADOS_QTD": ajuizados_qtd,
        "AJUIZADOS_CNPJS": ajuizados_cnpjs,
        "AJUIZADOS_DIVIDA": ajuizados_divida,
        "SG_QTD": garantias_metricas["SEGURO GARANTIA"]["qtd"],
        "SG_CNPJS": garantias_metricas["SEGURO GARANTIA"]["cnpjs"],
        "SG_DIVIDA": garantias_metricas["SEGURO GARANTIA"]["divida"],
        "CARTA_QTD": garantias_metricas["CARTA FIANCA"]["qtd"],
        "CARTA_CNPJS": garantias_metricas["CARTA FIANCA"]["cnpjs"],
        "CARTA_DIVIDA": garantias_metricas["CARTA FIANCA"]["divida"],
        "DEPOSITO_QTD": garantias_metricas["DEPOSITO"]["qtd"],
        "DEPOSITO_CNPJS": garantias_metricas["DEPOSITO"]["cnpjs"],
        "DEPOSITO_DIVIDA": garantias_metricas["DEPOSITO"]["divida"],
        "PENHORA_QTD": garantias_metricas["PENHORA"]["qtd"],
        "PENHORA_CNPJS": garantias_metricas["PENHORA"]["cnpjs"],
        "PENHORA_DIVIDA": garantias_metricas["PENHORA"]["divida"],
        "SEM_QTD": garantias_metricas["SEM GARANTIA"]["qtd"],
        "SEM_CNPJS": garantias_metricas["SEM GARANTIA"]["cnpjs"],
        "SEM_DIVIDA": garantias_metricas["SEM GARANTIA"]["divida"],
        "SEM_MEDIA": sem_media,
        "SEM_MEDIANA": sem_mediana,
    }

    print(f"\n  RESUMO BASE {i}:")
    print(f"    CNPJs unicos: {cnpjs_total:,}")
    print(f"    Divida total: {fmt_brl(divida_total)}")
    print(f"    Ajuizados: {ajuizados_qtd:,} inscricoes | {ajuizados_cnpjs:,} CNPJs")
    for gar_nome, gar_dados in garantias_metricas.items():
        print(f"    -> {gar_nome}: {gar_dados['qtd']:,} ({gar_dados['cnpjs']:,} CNPJs) | {fmt_brl(gar_dados['divida'])}")

    return resultado


def gerar_graficos(df_result):
    """Gera painel de 6 graficos consolidados."""
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    cores = config.CORES_REGIOES

    # 1. Inscricoes por base
    ax1 = axes[0, 0]
    bars1 = ax1.bar(df_result["BASE"], df_result["TOTAL_FILTRADO"], color=cores)
    ax1.set_title("Inscricoes PJ + Tributos por Base", fontweight="bold")
    ax1.set_ylabel("Quantidade")
    ax1.tick_params(axis='x', rotation=45)
    for bar, val in zip(bars1, df_result["TOTAL_FILTRADO"]):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{val:,.0f}',
                 ha='center', va='bottom', fontsize=8)

    # 2. Ajuizados por base
    ax2 = axes[0, 1]
    bars2 = ax2.bar(df_result["BASE"], df_result["AJUIZADOS_QTD"], color=cores)
    ax2.set_title("Inscricoes Ajuizadas por Base", fontweight="bold")
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

    # 4. Divida sem garantia (em trilhoes)
    ax4 = axes[1, 0]
    divida_tri = df_result["SEM_DIVIDA"] / 1e12
    bars4 = ax4.bar(df_result["BASE"], divida_tri, color=cores)
    ax4.set_title("Divida Ajuizada SEM GARANTIA (R$ tri)", fontweight="bold")
    ax4.set_ylabel("R$ trilhoes")
    ax4.tick_params(axis='x', rotation=45)
    for bar, val in zip(bars4, divida_tri):
        ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{val:.1f}',
                 ha='center', va='bottom', fontsize=9)

    # 5. Tipo de garantia (total Brasil) - Pizza
    ax5 = axes[1, 1]
    garantias = ["Seguro Garantia", "Carta Fianca", "Deposito", "Penhora", "Sem Garantia"]
    valores_gar = [
        df_result["SG_QTD"].sum(),
        df_result["CARTA_QTD"].sum(),
        df_result["DEPOSITO_QTD"].sum(),
        df_result["PENHORA_QTD"].sum(),
        df_result["SEM_QTD"].sum(),
    ]
    cores_pizza = ['#27ae60', '#3498db', '#f39c12', '#e74c3c', '#95a5a6']
    explode = (0.05, 0.05, 0.05, 0.05, 0.1)
    ax5.pie(valores_gar, labels=garantias, autopct='%1.1f%%',
            colors=cores_pizza, explode=explode, startangle=90)
    ax5.set_title("Distribuicao por Tipo de Garantia (Ajuizados Brasil)", fontweight="bold")

    # 6. Comparativo CNPJs
    ax6 = axes[1, 2]
    x = np.arange(len(df_result))
    width = 0.35
    ax6.bar(x - width/2, df_result["AJUIZADOS_CNPJS"], width,
            label="Ajuizados Total", color='#3498db')
    ax6.bar(x + width/2, df_result["SEM_CNPJS"], width,
            label="Sem Garantia", color='#e74c3c')
    ax6.set_title("CNPJs: Ajuizados vs Sem Garantia", fontweight="bold")
    ax6.set_ylabel("CNPJs unicos")
    ax6.set_xticks(x)
    ax6.set_xticklabels(df_result["BASE"], rotation=45)
    ax6.legend()

    plt.suptitle("ANALISE PGFN BRASIL - 6 BASES CONSOLIDADAS",
                 fontsize=14, fontweight="bold", y=1.02)
    plt.tight_layout()
    return fig


def main():
    print("=" * 100)
    print("  PROCESSANDO AS 6 BASES PGFN - BRASIL COMPLETO")
    print("=" * 100)

    resultados = []
    for i, (arquivo, nome_curto, nome_completo) in enumerate(config.BASES, 1):
        try:
            resultado = processar_base(i, arquivo, nome_completo)
            resultados.append(resultado)
        except FileNotFoundError as e:
            print(f"  ERRO: {e}")
            continue
        except Exception as e:
            print(f"  ERRO ao processar base {i}: {e}")
            continue

    if not resultados:
        print("\nNenhuma base processada com sucesso.")
        sys.exit(1)

    df_result = pd.DataFrame(resultados)

    # Consolidacao
    print("\n" + "=" * 100)
    print("  CONSOLIDACAO - BRASIL COMPLETO")
    print("=" * 100)

    print(f"\n{'BASE':<10} {'FILTRADO':>12} {'CNPJs':>10} {'AJUIZ_QTD':>12} {'SEM_GAR_QTD':>14}")
    print("-" * 65)
    for _, row in df_result.iterrows():
        print(f"{row['BASE']:<10} {row['TOTAL_FILTRADO']:>12,} {row['CNPJS_TOTAL']:>10,} "
              f"{row['AJUIZADOS_QTD']:>12,} {row['SEM_QTD']:>14,}")
    print("-" * 65)
    print(f"{'TOTAL':<10} {df_result['TOTAL_FILTRADO'].sum():>12,} "
          f"{df_result['CNPJS_TOTAL'].sum():>10,} "
          f"{df_result['AJUIZADOS_QTD'].sum():>12,} "
          f"{df_result['SEM_QTD'].sum():>14,}")

    # Graficos
    print("\n  Gerando graficos...")
    fig = gerar_graficos(df_result)
    salvar_grafico(fig, "consolidado_6_bases_pgfn.png")
    plt.close(fig)

    # CSV
    salvar_csv(df_result, "consolidado_6_bases_pgfn.csv")

    # Resumo final
    total_sem = df_result["SEM_QTD"].sum()
    total_ajuizados = df_result["AJUIZADOS_QTD"].sum()
    total_sem_divida = df_result["SEM_DIVIDA"].sum()
    pct_sem = (total_sem / total_ajuizados * 100) if total_ajuizados > 0 else 0

    from utils import calcular_potencial_comercial
    premio, comissao = calcular_potencial_comercial(total_sem_divida)

    print("\n" + "=" * 100)
    print("  RESUMO BRASIL - MERCADO SEGURO GARANTIA JUDICIAL")
    print("=" * 100)
    print(f"\n  MERCADO ALVO (AJUIZADO + SEM GARANTIA):")
    print(f"    Inscricoes: {total_sem:,}")
    print(f"    CNPJs: {df_result['SEM_CNPJS'].sum():,}")
    print(f"    Divida: {fmt_brl(total_sem_divida)}")
    print(f"    {pct_sem:.2f}% dos ajuizados estao SEM GARANTIA")
    print(f"\n  POTENCIAL COMERCIAL:")
    print(f"    Premio total (2%): {fmt_brl(premio)}")
    print(f"    Comissao (25%): {fmt_brl(comissao)}")
    print("\n" + "=" * 100)
    print("  ANALISE CONCLUIDA!")
    print("=" * 100)


if __name__ == "__main__":
    main()
