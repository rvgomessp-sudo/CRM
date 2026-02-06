#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Análise Econométrica do Seguro Garantia na Base PGFN
=====================================================
Análise completa do comportamento do seguro garantia judicial
no universo da dívida ativa federal brasileira.

Portaria PGFN/MF nº 2.044/2024:
- Modalidade 1: Execução Fiscal
- Modalidade 2: Negociação Administrativa

Etapas:
1. Diagnóstico da base (OBRIGATÓRIO antes das demais)
2. Criação de variáveis derivadas
3. Exportação de arquivos enriquecidos
4. Análise comparativa intra-CNPJ (CNPJs mistos)

Autor: Claude Code
Data: 2025
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import warnings
import gc
import os
import re
from scipy import stats

warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

# Diretório de dados - usar variável de ambiente ou fallback
DATA_DIR = Path(os.environ.get("PGFN_DATA_DIR", r"C:\Rodrigo\BasePGFN\2025"))

# Arquivo de entrada (ajustar ano conforme necessário)
ARQUIVO_ENTRADA = "pgfn_unificada_cnpj_principal_semibruta.csv"

# Configurações de processamento
CHUNK_SIZE = 500_000  # Linhas por chunk
USE_CATEGORIES = True  # Usar dtype category para otimização

# Diretório de saída
OUTPUT_DIR = DATA_DIR / "outputs_econometria_sg"

# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def fmt_brl(valor):
    """Formata valor numérico para padrão brasileiro."""
    if pd.isna(valor) or valor == 0:
        return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def fmt_pct(valor):
    """Formata percentual."""
    if pd.isna(valor):
        return "N/A"
    return f"{valor*100:.2f}%"

def fmt_num(valor):
    """Formata número com separador de milhar."""
    if pd.isna(valor):
        return "N/A"
    return f"{valor:,.0f}".replace(",", ".")

def converter_valor_brl(valor_str):
    """Converte valor BR (1.234.567,89) para float."""
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

def detectar_encoding(caminho):
    """Tenta detectar encoding do arquivo."""
    encodings = ['latin-1', 'utf-8-sig', 'utf-8', 'cp1252']
    for enc in encodings:
        try:
            with open(caminho, 'r', encoding=enc) as f:
                f.read(10000)
            return enc
        except:
            continue
    return 'latin-1'

def detectar_separador(caminho, encoding):
    """Detecta separador do CSV."""
    with open(caminho, 'r', encoding=encoding) as f:
        primeira_linha = f.readline()
    if ';' in primeira_linha:
        return ';'
    elif '\t' in primeira_linha:
        return '\t'
    return ','

def detectar_coluna_cnpj(colunas):
    """Detecta o nome da coluna que contém o CNPJ."""
    candidatos = ['CPF_CNPJ', 'CNPJ', 'CPF_CNPJ_DEVEDOR', 'CNPJ_DEVEDOR', 'NR_CPF_CNPJ', 'CPF/CNPJ']
    for candidato in candidatos:
        if candidato in colunas:
            return candidato
    # Procurar por substring
    for col in colunas:
        col_upper = col.upper()
        if 'CNPJ' in col_upper or 'CPF' in col_upper:
            return col
    return None

def detectar_coluna_nome(colunas):
    """Detecta o nome da coluna que contém o nome do devedor."""
    candidatos = ['NOME_DEVEDOR', 'NOME', 'RAZAO_SOCIAL', 'DEVEDOR']
    for candidato in candidatos:
        if candidato in colunas:
            return candidato
    for col in colunas:
        col_upper = col.upper()
        if 'NOME' in col_upper and 'DEVEDOR' in col_upper:
            return col
    return None

# ============================================================================
# ETAPA 1 - DIAGNÓSTICO DA BASE
# ============================================================================

def executar_diagnostico(caminho_arquivo, output_dir):
    """
    Executa diagnóstico completo da base.
    OBRIGATÓRIO antes de prosseguir com as demais etapas.
    """
    print("\n" + "=" * 80)
    print("ETAPA 1 - DIAGNÓSTICO DA BASE PGFN")
    print("=" * 80)

    # Verificar arquivo
    if not caminho_arquivo.exists():
        print(f"[ERRO] Arquivo não encontrado: {caminho_arquivo}")
        return None

    # Detectar encoding e separador
    print("\n1.0 - Detectando formato do arquivo...")
    encoding = detectar_encoding(caminho_arquivo)
    separador = detectar_separador(caminho_arquivo, encoding)
    print(f"    Encoding detectado: {encoding}")
    print(f"    Separador detectado: '{separador}'")

    # Criar diretório de saída
    output_dir.mkdir(parents=True, exist_ok=True)

    # Buffer para relatório
    relatorio = []
    def log(texto=""):
        print(texto)
        relatorio.append(texto)

    log(f"\nArquivo: {caminho_arquivo}")
    log(f"Data análise: {datetime.now().strftime('%d/%m/%Y %H:%M')}")

    # Carregar amostra para diagnóstico inicial
    log("\n1.1 - Carregando amostra para diagnóstico...")

    # Primeiro, contar linhas totais
    log("    Contando linhas totais (pode demorar)...")
    total_linhas = sum(1 for _ in open(caminho_arquivo, 'r', encoding=encoding)) - 1
    log(f"    Total de linhas: {fmt_num(total_linhas)}")

    # Carregar em chunks para diagnóstico
    log("\n1.2 - Processando diagnóstico por chunks...")

    # Dicionários para acumular contagens
    contagens = {
        'SITUACAO_INSCRICAO': {},
        'TIPO_SITUACAO_INSCRICAO': {},
        'INDICADOR_AJUIZADO': {},
        'TIPO_DEVEDOR': {},
        'TIPO_PESSOA': {},
        'UF_DEVEDOR': {},
        'RECEITA_PRINCIPAL': {},
        'ANO_INSCRICAO': {}
    }

    # Para crosstab SITUACAO x AJUIZADO
    crosstab_sit_ajuiz = {}

    # Para estatísticas de valor
    valores = []
    valor_por_receita = {}

    # Para distribuição temporal
    anos_inscricao = {}

    # Detectar nomes das colunas no primeiro chunk
    col_cnpj = None
    col_nome = None
    todas_colunas = []

    chunks_processados = 0

    for chunk in pd.read_csv(
        caminho_arquivo,
        sep=separador,
        encoding=encoding,
        dtype=str,
        chunksize=CHUNK_SIZE,
        low_memory=False
    ):
        chunks_processados += 1

        # Detectar colunas no primeiro chunk
        if chunks_processados == 1:
            todas_colunas = list(chunk.columns)
            col_cnpj = detectar_coluna_cnpj(todas_colunas)
            col_nome = detectar_coluna_nome(todas_colunas)
            log(f"\n    Colunas detectadas: {len(todas_colunas)}")
            log(f"    Coluna CNPJ: {col_cnpj}")
            log(f"    Coluna Nome: {col_nome}")
            log(f"    Todas as colunas: {todas_colunas[:10]}..." if len(todas_colunas) > 10 else f"    Todas as colunas: {todas_colunas}")
        if chunks_processados % 10 == 0:
            log(f"    Processando chunk {chunks_processados}... ({chunks_processados * CHUNK_SIZE:,} linhas)")

        # Acumular contagens
        for col in contagens.keys():
            if col == 'ANO_INSCRICAO':
                continue
            if col in chunk.columns:
                vc = chunk[col].value_counts()
                for valor, count in vc.items():
                    contagens[col][valor] = contagens[col].get(valor, 0) + count

        # Extrair ano de inscrição
        if 'DATA_INSCRICAO' in chunk.columns:
            chunk['_ANO'] = pd.to_datetime(chunk['DATA_INSCRICAO'], errors='coerce').dt.year
            vc_ano = chunk['_ANO'].value_counts()
            for ano, count in vc_ano.items():
                if pd.notna(ano):
                    contagens['ANO_INSCRICAO'][int(ano)] = contagens['ANO_INSCRICAO'].get(int(ano), 0) + count

        # Crosstab SITUACAO x AJUIZADO
        if 'SITUACAO_INSCRICAO' in chunk.columns and 'INDICADOR_AJUIZADO' in chunk.columns:
            for _, row in chunk[['SITUACAO_INSCRICAO', 'INDICADOR_AJUIZADO']].iterrows():
                sit = row['SITUACAO_INSCRICAO']
                ajuiz = row['INDICADOR_AJUIZADO']
                key = (sit, ajuiz)
                crosstab_sit_ajuiz[key] = crosstab_sit_ajuiz.get(key, 0) + 1

        # Valores para estatísticas
        if 'VALOR_CONSOLIDADO_NUM' in chunk.columns:
            chunk['_VALOR'] = pd.to_numeric(chunk['VALOR_CONSOLIDADO_NUM'], errors='coerce')
        elif 'VALOR_CONSOLIDADO' in chunk.columns:
            chunk['_VALOR'] = chunk['VALOR_CONSOLIDADO'].apply(converter_valor_brl)
        else:
            chunk['_VALOR'] = 0.0

        # Amostrar valores (não guardar todos, seria muito)
        sample_valores = chunk['_VALOR'].dropna().sample(min(1000, len(chunk)), random_state=42).tolist()
        valores.extend(sample_valores)

        # Valor por receita
        if 'RECEITA_PRINCIPAL' in chunk.columns:
            for receita, grupo in chunk.groupby('RECEITA_PRINCIPAL'):
                if receita not in valor_por_receita:
                    valor_por_receita[receita] = {'count': 0, 'sum': 0.0}
                valor_por_receita[receita]['count'] += len(grupo)
                valor_por_receita[receita]['sum'] += grupo['_VALOR'].sum()

        # Liberar memória
        del chunk
        gc.collect()

    log(f"\n    Chunks processados: {chunks_processados}")

    # ========================================================================
    # GERAR RELATÓRIO DE DIAGNÓSTICO
    # ========================================================================

    log("\n" + "=" * 80)
    log("RESULTADOS DO DIAGNÓSTICO")
    log("=" * 80)

    # 1.1 - Value counts das variáveis-chave
    log("\n" + "-" * 60)
    log("1.1 - VALUE COUNTS DAS VARIÁVEIS-CHAVE")
    log("-" * 60)

    for col, vc in contagens.items():
        if not vc:
            continue
        log(f"\n>>> {col}")
        log(f"    Valores únicos: {len(vc)}")

        # Ordenar por frequência
        sorted_vc = sorted(vc.items(), key=lambda x: x[1], reverse=True)

        # Mostrar top valores
        top_n = 30 if col in ['SITUACAO_INSCRICAO', 'RECEITA_PRINCIPAL'] else 15
        for i, (valor, count) in enumerate(sorted_vc[:top_n]):
            pct = count / total_linhas * 100
            log(f"    {i+1:3}. {valor}: {fmt_num(count)} ({pct:.2f}%)")

        if len(sorted_vc) > top_n:
            log(f"    ... e mais {len(sorted_vc) - top_n} valores")

    # 1.2 - Crosstab SITUACAO x AJUIZADO
    log("\n" + "-" * 60)
    log("1.2 - CROSSTAB: SITUACAO_INSCRICAO x INDICADOR_AJUIZADO")
    log("-" * 60)

    # Converter para DataFrame
    if crosstab_sit_ajuiz:
        situacoes = sorted(set(k[0] for k in crosstab_sit_ajuiz.keys()))
        ajuizados = sorted(set(k[1] for k in crosstab_sit_ajuiz.keys()))

        # Criar tabela resumida (top 20 situações por frequência)
        sit_totals = {}
        for (sit, ajuiz), count in crosstab_sit_ajuiz.items():
            sit_totals[sit] = sit_totals.get(sit, 0) + count

        top_situacoes = sorted(sit_totals.items(), key=lambda x: x[1], reverse=True)[:20]

        log(f"\n{'SITUACAO':<50} | {'SIM':>12} | {'NAO':>12} | {'TOTAL':>12}")
        log("-" * 90)

        for sit, total in top_situacoes:
            sim = crosstab_sit_ajuiz.get((sit, 'SIM'), 0) + crosstab_sit_ajuiz.get((sit, 'Sim'), 0)
            nao = crosstab_sit_ajuiz.get((sit, 'NAO'), 0) + crosstab_sit_ajuiz.get((sit, 'Nao'), 0) + crosstab_sit_ajuiz.get((sit, 'NÃO'), 0)
            outros = total - sim - nao
            if outros > 0:
                nao += outros  # Assumir que outros são NÃO
            log(f"{sit[:50]:<50} | {fmt_num(sim):>12} | {fmt_num(nao):>12} | {fmt_num(total):>12}")

    # 1.3 - Top 30 RECEITA_PRINCIPAL por valor
    log("\n" + "-" * 60)
    log("1.3 - TOP 30 RECEITA_PRINCIPAL POR VALOR TOTAL")
    log("-" * 60)

    if valor_por_receita:
        # Calcular métricas
        receitas_stats = []
        for receita, stats in valor_por_receita.items():
            media = stats['sum'] / stats['count'] if stats['count'] > 0 else 0
            receitas_stats.append({
                'receita': receita,
                'contagem': stats['count'],
                'valor_total': stats['sum'],
                'valor_medio': media
            })

        receitas_df = pd.DataFrame(receitas_stats)
        receitas_df = receitas_df.sort_values('valor_total', ascending=False).head(30)

        log(f"\n{'RECEITA':<45} | {'CONTAGEM':>12} | {'VALOR TOTAL':>18} | {'MÉDIA':>15}")
        log("-" * 100)

        for _, row in receitas_df.iterrows():
            log(f"{str(row['receita'])[:45]:<45} | {fmt_num(row['contagem']):>12} | {fmt_brl(row['valor_total']):>18} | {fmt_brl(row['valor_medio']):>15}")

        # Salvar como CSV
        receitas_df.to_csv(output_dir / "diagnostico_receita_principal.csv", sep=";", index=False, encoding="utf-8-sig")

    # 1.4 - Estatísticas descritivas de VALOR
    log("\n" + "-" * 60)
    log("1.4 - ESTATÍSTICAS DESCRITIVAS DE VALOR_CONSOLIDADO")
    log("-" * 60)

    if valores:
        valores_arr = np.array(valores)
        log(f"\n    Amostra: {len(valores_arr):,} valores")
        log(f"    Mínimo: {fmt_brl(np.min(valores_arr))}")
        log(f"    Máximo: {fmt_brl(np.max(valores_arr))}")
        log(f"    Média: {fmt_brl(np.mean(valores_arr))}")
        log(f"    Mediana: {fmt_brl(np.median(valores_arr))}")
        log(f"    Desvio Padrão: {fmt_brl(np.std(valores_arr))}")
        log(f"\n    Percentis:")
        for p in [10, 25, 50, 75, 90, 95, 99]:
            log(f"      P{p}: {fmt_brl(np.percentile(valores_arr, p))}")

    # 1.5 - Distribuição temporal
    log("\n" + "-" * 60)
    log("1.5 - DISTRIBUIÇÃO TEMPORAL (ANO DE INSCRIÇÃO)")
    log("-" * 60)

    if contagens['ANO_INSCRICAO']:
        anos_sorted = sorted(contagens['ANO_INSCRICAO'].items())
        log(f"\n{'ANO':>6} | {'CONTAGEM':>15} | {'%':>8}")
        log("-" * 35)
        for ano, count in anos_sorted:
            pct = count / total_linhas * 100
            log(f"{ano:>6} | {fmt_num(count):>15} | {pct:>7.2f}%")

    # ========================================================================
    # ANÁLISE DE PADRÕES PARA FLAG_GARANTIDA
    # ========================================================================

    log("\n" + "=" * 80)
    log("ANÁLISE PARA DEFINIÇÃO DA FLAG_GARANTIDA")
    log("=" * 80)

    log("\nPadrões identificados em SITUACAO_INSCRICAO:")

    # Procurar padrões relacionados a garantia
    garantia_patterns = [
        'GARANTID', 'SEGURO', 'SUSPENS', 'CAUCAO', 'PENHORA',
        'DEPOSITO', 'FIANCA', 'BLOQUEIO', 'BEM', 'CAUCION'
    ]

    situacoes = contagens.get('SITUACAO_INSCRICAO', {})

    for pattern in garantia_patterns:
        matches = [(sit, count) for sit, count in situacoes.items()
                   if pattern.upper() in str(sit).upper()]
        if matches:
            log(f"\n  Padrão '{pattern}':")
            for sit, count in sorted(matches, key=lambda x: x[1], reverse=True)[:10]:
                pct = count / total_linhas * 100
                log(f"    - {sit}: {fmt_num(count)} ({pct:.3f}%)")

    log("\n" + "-" * 60)
    log("RECOMENDAÇÕES PARA FLAG_GARANTIDA:")
    log("-" * 60)
    log("""
    Baseado nos padrões acima, defina FLAG_GARANTIDA = 1 para situações que contenham:
    - 'GARANTIA' ou 'GARANTIDO'
    - 'SEGURO' (provavelmente seguro garantia)
    - 'SUSPENS' (pode indicar suspensão por garantia)
    - 'PENHORA' (garantia por penhora)
    - 'DEPOSITO' (depósito judicial)
    - 'FIANCA' (carta de fiança)

    IMPORTANTE: Revise os valores acima para confirmar quais situações
    realmente indicam garantia antes de prosseguir com a Etapa 2.
    """)

    # Salvar relatório
    caminho_relatorio = output_dir / "diagnostico_base_pgfn.txt"
    with open(caminho_relatorio, "w", encoding="utf-8") as f:
        f.write("\n".join(relatorio))

    print(f"\n[OK] Diagnóstico salvo: {caminho_relatorio}")

    # Salvar contagens como CSVs auxiliares
    for col, vc in contagens.items():
        if vc:
            df_vc = pd.DataFrame([
                {'valor': k, 'contagem': v, 'percentual': v/total_linhas*100}
                for k, v in sorted(vc.items(), key=lambda x: x[1], reverse=True)
            ])
            df_vc.to_csv(output_dir / f"diagnostico_{col.lower()}.csv", sep=";", index=False, encoding="utf-8-sig")

    # Retornar informações para próximas etapas
    return {
        'total_linhas': total_linhas,
        'encoding': encoding,
        'separador': separador,
        'contagens': contagens,
        'situacoes': situacoes,
        'col_cnpj': col_cnpj,
        'col_nome': col_nome,
        'colunas': todas_colunas
    }

# ============================================================================
# ETAPA 2 - CRIAÇÃO DE VARIÁVEIS DERIVADAS
# ============================================================================

def definir_flag_garantida(situacao):
    """
    Define FLAG_GARANTIDA baseado na SITUACAO_INSCRICAO.
    AJUSTAR esta função após análise do diagnóstico (Etapa 1).
    """
    if pd.isna(situacao):
        return 0

    sit_upper = str(situacao).upper()

    # Padrões que indicam GARANTIA
    # TODO: Ajustar após análise do diagnóstico
    padroes_garantia = [
        'GARANTIA',
        'GARANTIDO',
        'GARANTID',
        'SEGURO',
        'SUSPENS',  # Pode indicar suspensão por garantia
        'CAUCAO',
        'CAUCION',
    ]

    for padrao in padroes_garantia:
        if padrao in sit_upper:
            return 1

    return 0

def definir_tipo_garantia(situacao):
    """
    Classifica o tipo de garantia quando FLAG_GARANTIDA = 1.
    """
    if pd.isna(situacao):
        return 'SEM_GARANTIA'

    sit_upper = str(situacao).upper()

    if 'SEGURO' in sit_upper:
        return 'SEGURO_GARANTIA'
    elif 'PENHORA' in sit_upper:
        return 'PENHORA'
    elif 'DEPOSITO' in sit_upper or 'DEPÓSITO' in sit_upper:
        return 'DEPOSITO_JUDICIAL'
    elif 'FIANCA' in sit_upper or 'FIANÇA' in sit_upper:
        return 'CARTA_FIANCA'
    elif 'CAUCAO' in sit_upper or 'CAUCION' in sit_upper:
        return 'CAUCAO'
    elif 'GARANTIA' in sit_upper or 'GARANTIDO' in sit_upper:
        return 'OUTRA_GARANTIA'
    elif 'SUSPENS' in sit_upper:
        return 'SUSPENSAO'  # Pode ou não ser garantia
    else:
        return 'SEM_GARANTIA'

def definir_grupo_tributario(receita):
    """
    Mapeia RECEITA_PRINCIPAL para grupos tributários simplificados.
    """
    if pd.isna(receita):
        return 'OUTROS'

    receita_upper = str(receita).upper()

    # Previdenciário
    if any(x in receita_upper for x in ['PREVIDEN', 'INSS', 'CONTRIB', 'PATRONAL']):
        return 'PREVIDENCIARIO'

    # IRPJ/CSLL
    if 'IRPJ' in receita_upper or 'CSLL' in receita_upper:
        return 'IRPJ_CSLL'

    # COFINS/PIS
    if 'COFINS' in receita_upper or 'PIS' in receita_upper or 'PASEP' in receita_upper:
        return 'COFINS_PIS'

    # IPI
    if 'IPI' in receita_upper:
        return 'IPI'

    # Simples Nacional
    if 'SIMPLES' in receita_upper:
        return 'SIMPLES'

    # FGTS
    if 'FGTS' in receita_upper:
        return 'FGTS'

    # Multas
    if 'MULTA' in receita_upper:
        return 'MULTAS'

    # ITR
    if 'ITR' in receita_upper or 'RURAL' in receita_upper:
        return 'ITR'

    # Imposto de Importação
    if 'IMPORT' in receita_upper or 'II -' in receita_upper:
        return 'IMPORTACAO'

    # IOF
    if 'IOF' in receita_upper:
        return 'IOF'

    return 'OUTROS'

def processar_chunk_etapa2(chunk, data_referencia, col_cnpj='CPF_CNPJ'):
    """
    Processa um chunk adicionando variáveis derivadas.
    """
    # 2.1 - Variáveis no nível da INSCRIÇÃO

    # Criar coluna padronizada CPF_CNPJ se necessário
    if col_cnpj != 'CPF_CNPJ' and col_cnpj in chunk.columns:
        chunk['CPF_CNPJ'] = chunk[col_cnpj]

    # Data como datetime
    chunk['DATA_INSCRICAO_DT'] = pd.to_datetime(chunk['DATA_INSCRICAO'], errors='coerce')

    # Idade em meses
    chunk['IDADE_INSCRICAO_MESES'] = (
        (data_referencia - chunk['DATA_INSCRICAO_DT']).dt.days / 30.44
    ).round(0)

    # Ano de inscrição
    chunk['ANO_INSCRICAO'] = chunk['DATA_INSCRICAO_DT'].dt.year

    # Garantir que VALOR_CONSOLIDADO_NUM existe e é numérico
    if 'VALOR_CONSOLIDADO_NUM' in chunk.columns:
        chunk['VALOR_CONSOLIDADO_NUM'] = pd.to_numeric(chunk['VALOR_CONSOLIDADO_NUM'], errors='coerce').fillna(0)
    elif 'VALOR_CONSOLIDADO' in chunk.columns:
        chunk['VALOR_CONSOLIDADO_NUM'] = chunk['VALOR_CONSOLIDADO'].apply(converter_valor_brl)
    else:
        chunk['VALOR_CONSOLIDADO_NUM'] = 0.0

    # Faixa de valor
    chunk['FAIXA_VALOR'] = pd.cut(
        chunk['VALOR_CONSOLIDADO_NUM'],
        bins=[0, 10_000, 50_000, 100_000, 500_000, 2_000_000, 10_000_000, 50_000_000, float('inf')],
        labels=['Até 10k', '10k-50k', '50k-100k', '100k-500k', '500k-2M', '2M-10M', '10M-50M', 'Acima 50M'],
        right=True
    )

    # Log do valor
    chunk['LOG_VALOR'] = np.log1p(chunk['VALOR_CONSOLIDADO_NUM'])

    # FLAG_GARANTIDA
    chunk['FLAG_GARANTIDA'] = chunk['SITUACAO_INSCRICAO'].apply(definir_flag_garantida).astype('int8')

    # TIPO_GARANTIA
    chunk['TIPO_GARANTIA'] = chunk['SITUACAO_INSCRICAO'].apply(definir_tipo_garantia)

    # FLAG_AJUIZADO binária
    if 'INDICADOR_AJUIZADO' in chunk.columns:
        chunk['FLAG_AJUIZADO'] = (
            chunk['INDICADOR_AJUIZADO'].fillna('').str.upper().str.strip().isin(['SIM', 'S', '1', 'TRUE'])
        ).astype('int8')
    else:
        chunk['FLAG_AJUIZADO'] = 0

    # Grupo tributário
    chunk['GRUPO_TRIBUTARIO'] = chunk['RECEITA_PRINCIPAL'].apply(definir_grupo_tributario)

    return chunk

def executar_etapa2(caminho_arquivo, diagnostico, output_dir):
    """
    Executa Etapa 2: Criação de variáveis derivadas.
    Processa em chunks e salva arquivo enriquecido.
    """
    print("\n" + "=" * 80)
    print("ETAPA 2 - CRIAÇÃO DE VARIÁVEIS DERIVADAS")
    print("=" * 80)

    encoding = diagnostico['encoding']
    separador = diagnostico['separador']
    total_linhas = diagnostico['total_linhas']
    col_cnpj = diagnostico.get('col_cnpj', 'CPF_CNPJ')
    col_nome = diagnostico.get('col_nome', 'NOME_DEVEDOR')

    print(f"\n  Usando coluna CNPJ: {col_cnpj}")
    print(f"  Usando coluna Nome: {col_nome}")

    data_referencia = pd.Timestamp.now()
    print(f"  Data de referência: {data_referencia.strftime('%d/%m/%Y')}")

    # Arquivo de saída temporário
    caminho_saida = output_dir / "pgfn_unificada_enriquecida_seguro_garantia.csv"

    # Processar em chunks
    print(f"\nProcessando {fmt_num(total_linhas)} linhas em chunks de {fmt_num(CHUNK_SIZE)}...")

    # Para agregação por CNPJ
    agg_cnpj_parcial = {}

    primeiro_chunk = True
    chunks_processados = 0

    for chunk in pd.read_csv(
        caminho_arquivo,
        sep=separador,
        encoding=encoding,
        dtype=str,
        chunksize=CHUNK_SIZE,
        low_memory=False
    ):
        chunks_processados += 1

        # Processar chunk
        chunk = processar_chunk_etapa2(chunk, data_referencia, col_cnpj)

        # Salvar chunk no arquivo de saída
        if primeiro_chunk:
            chunk.to_csv(caminho_saida, sep=";", index=False, encoding="utf-8-sig", mode='w')
            primeiro_chunk = False
        else:
            chunk.to_csv(caminho_saida, sep=";", index=False, encoding="utf-8-sig", mode='a', header=False)

        # Acumular agregações por CNPJ (usa coluna padronizada CPF_CNPJ criada no processamento)
        if 'CPF_CNPJ' not in chunk.columns:
            print(f"  [ERRO] Coluna 'CPF_CNPJ' não encontrada no chunk após processamento!")
            print(f"  Colunas disponíveis: {list(chunk.columns)}")
            break

        for cpf_cnpj, grupo in chunk.groupby('CPF_CNPJ'):
            if cpf_cnpj not in agg_cnpj_parcial:
                agg_cnpj_parcial[cpf_cnpj] = {
                    'TOTAL_INSCRICOES': 0,
                    'VALOR_TOTAL': 0.0,
                    'TOTAL_AJUIZADO': 0,
                    'TOTAL_GARANTIDAS': 0,
                    'UFS': set(),
                    'RECEITAS': set(),
                    'DATA_MIN': None,
                    'DATA_MAX': None,
                    'VALORES': [],
                    'NOME': None
                }

            agg = agg_cnpj_parcial[cpf_cnpj]
            agg['TOTAL_INSCRICOES'] += len(grupo)
            agg['VALOR_TOTAL'] += grupo['VALOR_CONSOLIDADO_NUM'].sum()
            agg['TOTAL_AJUIZADO'] += grupo['FLAG_AJUIZADO'].sum()
            agg['TOTAL_GARANTIDAS'] += grupo['FLAG_GARANTIDA'].sum()

            if 'UF_DEVEDOR' in grupo.columns:
                agg['UFS'].update(grupo['UF_DEVEDOR'].dropna().unique())
            if 'RECEITA_PRINCIPAL' in grupo.columns:
                agg['RECEITAS'].update(grupo['RECEITA_PRINCIPAL'].dropna().unique())

            datas = grupo['DATA_INSCRICAO_DT'].dropna()
            if len(datas) > 0:
                data_min = datas.min()
                data_max = datas.max()
                if agg['DATA_MIN'] is None or data_min < agg['DATA_MIN']:
                    agg['DATA_MIN'] = data_min
                if agg['DATA_MAX'] is None or data_max > agg['DATA_MAX']:
                    agg['DATA_MAX'] = data_max

            # Guardar alguns valores para calcular mediana depois (limitado)
            if len(agg['VALORES']) < 100:
                agg['VALORES'].extend(grupo['VALOR_CONSOLIDADO_NUM'].head(10).tolist())

            # Nome do devedor
            if agg['NOME'] is None and col_nome in grupo.columns:
                agg['NOME'] = grupo[col_nome].iloc[0]

        if chunks_processados % 5 == 0:
            print(f"  Chunk {chunks_processados}: {chunks_processados * CHUNK_SIZE:,} linhas processadas...")
            gc.collect()

        del chunk
        gc.collect()

    print(f"\n[OK] Arquivo enriquecido salvo: {caminho_saida}")

    # Criar tabela agregada por CNPJ
    print("\nCriando tabela agregada por CNPJ...")

    agg_rows = []
    for cpf_cnpj, agg in agg_cnpj_parcial.items():
        valores = agg['VALORES']
        valor_mediano = np.median(valores) if valores else 0
        valor_medio = agg['VALOR_TOTAL'] / agg['TOTAL_INSCRICOES'] if agg['TOTAL_INSCRICOES'] > 0 else 0

        span_meses = 0
        if agg['DATA_MIN'] and agg['DATA_MAX']:
            span_meses = (agg['DATA_MAX'] - agg['DATA_MIN']).days / 30.44

        taxa_garantia = agg['TOTAL_GARANTIDAS'] / agg['TOTAL_INSCRICOES'] if agg['TOTAL_INSCRICOES'] > 0 else 0
        taxa_ajuizamento = agg['TOTAL_AJUIZADO'] / agg['TOTAL_INSCRICOES'] if agg['TOTAL_INSCRICOES'] > 0 else 0

        flag_misto = 1 if (agg['TOTAL_GARANTIDAS'] > 0 and agg['TOTAL_GARANTIDAS'] < agg['TOTAL_INSCRICOES']) else 0

        # Faixa de dívida total
        valor_total = agg['VALOR_TOTAL']
        if valor_total <= 100_000:
            faixa = 'Até 100k'
        elif valor_total <= 1_000_000:
            faixa = '100k-1M'
        elif valor_total <= 10_000_000:
            faixa = '1M-10M'
        elif valor_total <= 100_000_000:
            faixa = '10M-100M'
        elif valor_total <= 1_000_000_000:
            faixa = '100M-1B'
        else:
            faixa = 'Acima 1B'

        agg_rows.append({
            'CPF_CNPJ': cpf_cnpj,
            'NOME_DEVEDOR': agg['NOME'],
            'TOTAL_INSCRICOES_CNPJ': agg['TOTAL_INSCRICOES'],
            'VALOR_TOTAL_CNPJ': agg['VALOR_TOTAL'],
            'VALOR_MEDIO_CNPJ': valor_medio,
            'VALOR_MEDIANO_CNPJ': valor_mediano,
            'TAXA_AJUIZAMENTO_CNPJ': taxa_ajuizamento,
            'TOTAL_AJUIZADO_CNPJ': agg['TOTAL_AJUIZADO'],
            'QTD_UF_CNPJ': len(agg['UFS']),
            'QTD_RECEITAS_CNPJ': len(agg['RECEITAS']),
            'INSCRICAO_MAIS_ANTIGA': agg['DATA_MIN'],
            'INSCRICAO_MAIS_RECENTE': agg['DATA_MAX'],
            'SPAN_TEMPORAL_CNPJ_MESES': round(span_meses, 0),
            'TOTAL_GARANTIDAS_CNPJ': agg['TOTAL_GARANTIDAS'],
            'TAXA_GARANTIA_CNPJ': taxa_garantia,
            'FLAG_MISTO_GARANTIA': flag_misto,
            'FAIXA_TOTAL_DIVIDA_CNPJ': faixa
        })

    df_agg = pd.DataFrame(agg_rows)
    df_agg = df_agg.sort_values('VALOR_TOTAL_CNPJ', ascending=False)

    caminho_agg = output_dir / "pgfn_agregado_cnpj_perfil_garantia.csv"
    df_agg.to_csv(caminho_agg, sep=";", index=False, encoding="utf-8-sig")

    print(f"[OK] Agregado por CNPJ salvo: {caminho_agg}")
    print(f"    Total de CNPJs únicos: {fmt_num(len(df_agg))}")

    return df_agg, agg_cnpj_parcial

# ============================================================================
# ETAPA 3 - TABELAS DE CRUZAMENTO
# ============================================================================

def executar_etapa3(caminho_enriquecido, output_dir, diagnostico):
    """
    Gera tabelas de cruzamento para análise.
    """
    print("\n" + "=" * 80)
    print("ETAPA 3 - TABELAS DE CRUZAMENTO")
    print("=" * 80)

    encoding = 'utf-8-sig'  # Arquivo enriquecido usa utf-8
    separador = ';'

    crosstabs_dir = output_dir / "crosstabs_garantia"
    crosstabs_dir.mkdir(exist_ok=True)

    # Acumular dados para crosstabs
    crosstabs = {
        'faixa_valor_x_garantida': {},
        'grupo_trib_x_garantida': {},
        'uf_x_garantida': {},
        'ano_x_garantida': {},
        'ajuizado_x_garantida': {},
        'tipo_garantia_x_ajuizado': {},
    }

    print("\nProcessando crosstabs por chunks...")

    chunks_processados = 0
    for chunk in pd.read_csv(
        caminho_enriquecido,
        sep=separador,
        encoding=encoding,
        dtype=str,
        chunksize=CHUNK_SIZE,
        low_memory=False
    ):
        chunks_processados += 1

        # Converter colunas necessárias
        chunk['FLAG_GARANTIDA'] = pd.to_numeric(chunk['FLAG_GARANTIDA'], errors='coerce').fillna(0).astype(int)
        chunk['FLAG_AJUIZADO'] = pd.to_numeric(chunk['FLAG_AJUIZADO'], errors='coerce').fillna(0).astype(int)
        chunk['VALOR_CONSOLIDADO_NUM'] = pd.to_numeric(chunk['VALOR_CONSOLIDADO_NUM'], errors='coerce').fillna(0)

        # A) FAIXA_VALOR x FLAG_GARANTIDA
        for (faixa, garantida), grupo in chunk.groupby(['FAIXA_VALOR', 'FLAG_GARANTIDA']):
            key = (faixa, garantida)
            if key not in crosstabs['faixa_valor_x_garantida']:
                crosstabs['faixa_valor_x_garantida'][key] = {'count': 0, 'valor': 0}
            crosstabs['faixa_valor_x_garantida'][key]['count'] += len(grupo)
            crosstabs['faixa_valor_x_garantida'][key]['valor'] += grupo['VALOR_CONSOLIDADO_NUM'].sum()

        # B) GRUPO_TRIBUTARIO x FLAG_GARANTIDA
        for (grupo_trib, garantida), grupo in chunk.groupby(['GRUPO_TRIBUTARIO', 'FLAG_GARANTIDA']):
            key = (grupo_trib, garantida)
            if key not in crosstabs['grupo_trib_x_garantida']:
                crosstabs['grupo_trib_x_garantida'][key] = {'count': 0, 'valor': 0}
            crosstabs['grupo_trib_x_garantida'][key]['count'] += len(grupo)
            crosstabs['grupo_trib_x_garantida'][key]['valor'] += grupo['VALOR_CONSOLIDADO_NUM'].sum()

        # C) UF x FLAG_GARANTIDA
        if 'UF_DEVEDOR' in chunk.columns:
            for (uf, garantida), grupo in chunk.groupby(['UF_DEVEDOR', 'FLAG_GARANTIDA']):
                key = (uf, garantida)
                if key not in crosstabs['uf_x_garantida']:
                    crosstabs['uf_x_garantida'][key] = {'count': 0, 'valor': 0}
                crosstabs['uf_x_garantida'][key]['count'] += len(grupo)
                crosstabs['uf_x_garantida'][key]['valor'] += grupo['VALOR_CONSOLIDADO_NUM'].sum()

        # D) ANO x FLAG_GARANTIDA
        if 'ANO_INSCRICAO' in chunk.columns:
            for (ano, garantida), grupo in chunk.groupby(['ANO_INSCRICAO', 'FLAG_GARANTIDA']):
                key = (ano, garantida)
                if key not in crosstabs['ano_x_garantida']:
                    crosstabs['ano_x_garantida'][key] = {'count': 0, 'valor': 0}
                crosstabs['ano_x_garantida'][key]['count'] += len(grupo)
                crosstabs['ano_x_garantida'][key]['valor'] += grupo['VALOR_CONSOLIDADO_NUM'].sum()

        # E) FLAG_AJUIZADO x FLAG_GARANTIDA
        for (ajuizado, garantida), grupo in chunk.groupby(['FLAG_AJUIZADO', 'FLAG_GARANTIDA']):
            key = (ajuizado, garantida)
            if key not in crosstabs['ajuizado_x_garantida']:
                crosstabs['ajuizado_x_garantida'][key] = {'count': 0, 'valor': 0}
            crosstabs['ajuizado_x_garantida'][key]['count'] += len(grupo)
            crosstabs['ajuizado_x_garantida'][key]['valor'] += grupo['VALOR_CONSOLIDADO_NUM'].sum()

        # F) TIPO_GARANTIA x FLAG_AJUIZADO
        if 'TIPO_GARANTIA' in chunk.columns:
            for (tipo, ajuizado), grupo in chunk.groupby(['TIPO_GARANTIA', 'FLAG_AJUIZADO']):
                key = (tipo, ajuizado)
                if key not in crosstabs['tipo_garantia_x_ajuizado']:
                    crosstabs['tipo_garantia_x_ajuizado'][key] = {'count': 0, 'valor': 0}
                crosstabs['tipo_garantia_x_ajuizado'][key]['count'] += len(grupo)
                crosstabs['tipo_garantia_x_ajuizado'][key]['valor'] += grupo['VALOR_CONSOLIDADO_NUM'].sum()

        if chunks_processados % 10 == 0:
            print(f"  Chunk {chunks_processados} processado...")

        del chunk
        gc.collect()

    # Converter para DataFrames e salvar
    print("\nSalvando crosstabs...")

    for nome, dados in crosstabs.items():
        if not dados:
            continue

        rows = []
        for (dim1, dim2), valores in dados.items():
            rows.append({
                'dimensao1': dim1,
                'dimensao2': dim2,
                'contagem': valores['count'],
                'valor_total': valores['valor']
            })

        df = pd.DataFrame(rows)

        # Calcular percentuais
        total = df['contagem'].sum()
        df['pct_contagem'] = df['contagem'] / total * 100
        df['pct_valor'] = df['valor_total'] / df['valor_total'].sum() * 100

        caminho = crosstabs_dir / f"{nome}.csv"
        df.to_csv(caminho, sep=";", index=False, encoding="utf-8-sig")
        print(f"  [OK] {caminho.name}")

    print(f"\n[OK] Crosstabs salvos em: {crosstabs_dir}")

# ============================================================================
# ETAPA 4 - ANÁLISE COMPARATIVA INTRA-CNPJ (MISTOS)
# ============================================================================

def executar_etapa4(caminho_enriquecido, df_agg, output_dir):
    """
    Análise comparativa para CNPJs mistos (têm garantidas e não garantidas).
    """
    print("\n" + "=" * 80)
    print("ETAPA 4 - ANÁLISE COMPARATIVA INTRA-CNPJ (MISTOS)")
    print("=" * 80)

    # Identificar CNPJs mistos
    cnpjs_mistos = set(df_agg[df_agg['FLAG_MISTO_GARANTIA'] == 1]['CPF_CNPJ'].tolist())

    print(f"\nCNPJs com perfil misto (garantidas + não garantidas): {fmt_num(len(cnpjs_mistos))}")

    if not cnpjs_mistos:
        print("[!] Nenhum CNPJ misto encontrado.")
        return

    # Acumular estatísticas
    stats_garantida = {'valores': [], 'idades': [], 'ajuizados': 0, 'total': 0, 'grupos': {}}
    stats_nao_garantida = {'valores': [], 'idades': [], 'ajuizados': 0, 'total': 0, 'grupos': {}}

    print("\nProcessando inscrições de CNPJs mistos...")

    chunks_processados = 0
    for chunk in pd.read_csv(
        caminho_enriquecido,
        sep=';',
        encoding='utf-8-sig',
        dtype=str,
        chunksize=CHUNK_SIZE,
        low_memory=False
    ):
        chunks_processados += 1

        # Filtrar apenas CNPJs mistos
        chunk = chunk[chunk['CPF_CNPJ'].isin(cnpjs_mistos)].copy()

        if chunk.empty:
            continue

        # Converter colunas
        chunk['FLAG_GARANTIDA'] = pd.to_numeric(chunk['FLAG_GARANTIDA'], errors='coerce').fillna(0).astype(int)
        chunk['FLAG_AJUIZADO'] = pd.to_numeric(chunk['FLAG_AJUIZADO'], errors='coerce').fillna(0).astype(int)
        chunk['VALOR_CONSOLIDADO_NUM'] = pd.to_numeric(chunk['VALOR_CONSOLIDADO_NUM'], errors='coerce').fillna(0)
        chunk['IDADE_INSCRICAO_MESES'] = pd.to_numeric(chunk['IDADE_INSCRICAO_MESES'], errors='coerce').fillna(0)

        # Separar garantidas e não garantidas
        garantidas = chunk[chunk['FLAG_GARANTIDA'] == 1]
        nao_garantidas = chunk[chunk['FLAG_GARANTIDA'] == 0]

        # Acumular estatísticas
        for df_subset, stats in [(garantidas, stats_garantida), (nao_garantidas, stats_nao_garantida)]:
            if df_subset.empty:
                continue

            # Amostrar valores (limitar para não estourar memória)
            if len(stats['valores']) < 100000:
                stats['valores'].extend(df_subset['VALOR_CONSOLIDADO_NUM'].head(1000).tolist())
            if len(stats['idades']) < 100000:
                stats['idades'].extend(df_subset['IDADE_INSCRICAO_MESES'].head(1000).tolist())

            stats['ajuizados'] += df_subset['FLAG_AJUIZADO'].sum()
            stats['total'] += len(df_subset)

            # Por grupo tributário
            if 'GRUPO_TRIBUTARIO' in df_subset.columns:
                for grupo, count in df_subset['GRUPO_TRIBUTARIO'].value_counts().items():
                    stats['grupos'][grupo] = stats['grupos'].get(grupo, 0) + count

        if chunks_processados % 10 == 0:
            print(f"  Chunk {chunks_processados}...")

        del chunk
        gc.collect()

    # Gerar relatório comparativo
    relatorio = []
    r = relatorio.append

    r("=" * 80)
    r("ANÁLISE COMPARATIVA INTRA-CNPJ - PERFIL MISTO DE GARANTIA")
    r("=" * 80)
    r(f"\nData: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    r(f"CNPJs analisados: {fmt_num(len(cnpjs_mistos))}")

    r("\n" + "-" * 60)
    r("COMPARATIVO: INSCRIÇÕES GARANTIDAS vs NÃO GARANTIDAS")
    r("-" * 60)

    for label, stats in [("GARANTIDAS", stats_garantida), ("NÃO GARANTIDAS", stats_nao_garantida)]:
        r(f"\n>>> {label}")
        r(f"    Total de inscrições: {fmt_num(stats['total'])}")

        if stats['valores']:
            valores = np.array(stats['valores'])
            r(f"    Valor médio: {fmt_brl(np.mean(valores))}")
            r(f"    Valor mediano: {fmt_brl(np.median(valores))}")
            r(f"    Valor P25: {fmt_brl(np.percentile(valores, 25))}")
            r(f"    Valor P75: {fmt_brl(np.percentile(valores, 75))}")

        if stats['idades']:
            idades = np.array(stats['idades'])
            r(f"    Idade média (meses): {np.mean(idades):.1f}")
            r(f"    Idade mediana (meses): {np.median(idades):.1f}")

        taxa_ajuiz = stats['ajuizados'] / stats['total'] * 100 if stats['total'] > 0 else 0
        r(f"    Taxa de ajuizamento: {taxa_ajuiz:.2f}%")

        r("\n    Distribuição por grupo tributário:")
        total_grupos = sum(stats['grupos'].values())
        for grupo, count in sorted(stats['grupos'].items(), key=lambda x: x[1], reverse=True)[:10]:
            pct = count / total_grupos * 100 if total_grupos > 0 else 0
            r(f"      - {grupo}: {fmt_num(count)} ({pct:.1f}%)")

    # Testes estatísticos
    r("\n" + "-" * 60)
    r("TESTES ESTATÍSTICOS")
    r("-" * 60)

    if stats_garantida['valores'] and stats_nao_garantida['valores']:
        # Mann-Whitney para valores
        try:
            stat_mw, p_mw = stats.mannwhitneyu(
                stats_garantida['valores'][:10000],
                stats_nao_garantida['valores'][:10000],
                alternative='two-sided'
            )
            r(f"\nTeste de Mann-Whitney (Valor Consolidado):")
            r(f"  Estatística U: {stat_mw:.2f}")
            r(f"  p-valor: {p_mw:.6f}")
            r(f"  Conclusão: {'Diferença significativa' if p_mw < 0.05 else 'Diferença não significativa'} (α=0.05)")
        except Exception as e:
            r(f"\n[!] Erro no teste Mann-Whitney: {e}")

    if stats_garantida['idades'] and stats_nao_garantida['idades']:
        # Kolmogorov-Smirnov para idades
        try:
            stat_ks, p_ks = stats.ks_2samp(
                stats_garantida['idades'][:10000],
                stats_nao_garantida['idades'][:10000]
            )
            r(f"\nTeste de Kolmogorov-Smirnov (Idade da Inscrição):")
            r(f"  Estatística D: {stat_ks:.4f}")
            r(f"  p-valor: {p_ks:.6f}")
            r(f"  Conclusão: {'Distribuições diferentes' if p_ks < 0.05 else 'Distribuições similares'} (α=0.05)")
        except Exception as e:
            r(f"\n[!] Erro no teste KS: {e}")

    # Chi-quadrado para ajuizamento
    if stats_garantida['total'] > 0 and stats_nao_garantida['total'] > 0:
        try:
            # Tabela de contingência
            observed = np.array([
                [stats_garantida['ajuizados'], stats_garantida['total'] - stats_garantida['ajuizados']],
                [stats_nao_garantida['ajuizados'], stats_nao_garantida['total'] - stats_nao_garantida['ajuizados']]
            ])
            chi2, p_chi, dof, expected = stats.chi2_contingency(observed)
            r(f"\nTeste Chi-quadrado (Ajuizamento x Garantia):")
            r(f"  Estatística χ²: {chi2:.2f}")
            r(f"  Graus de liberdade: {dof}")
            r(f"  p-valor: {p_chi:.6f}")
            r(f"  Conclusão: {'Associação significativa' if p_chi < 0.05 else 'Sem associação significativa'} (α=0.05)")
        except Exception as e:
            r(f"\n[!] Erro no teste Chi-quadrado: {e}")

    r("\n" + "=" * 80)
    r("FIM DA ANÁLISE")
    r("=" * 80)

    # Salvar relatório
    caminho_relatorio = output_dir / "analise_cnpj_mistos.txt"
    with open(caminho_relatorio, "w", encoding="utf-8") as f:
        f.write("\n".join(relatorio))

    print("\n".join(relatorio))
    print(f"\n[OK] Análise salva: {caminho_relatorio}")

    # Salvar estatísticas como CSV
    stats_df = pd.DataFrame([
        {
            'categoria': 'GARANTIDAS',
            'total_inscricoes': stats_garantida['total'],
            'valor_medio': np.mean(stats_garantida['valores']) if stats_garantida['valores'] else 0,
            'valor_mediano': np.median(stats_garantida['valores']) if stats_garantida['valores'] else 0,
            'idade_media_meses': np.mean(stats_garantida['idades']) if stats_garantida['idades'] else 0,
            'taxa_ajuizamento': stats_garantida['ajuizados'] / stats_garantida['total'] if stats_garantida['total'] > 0 else 0
        },
        {
            'categoria': 'NAO_GARANTIDAS',
            'total_inscricoes': stats_nao_garantida['total'],
            'valor_medio': np.mean(stats_nao_garantida['valores']) if stats_nao_garantida['valores'] else 0,
            'valor_mediano': np.median(stats_nao_garantida['valores']) if stats_nao_garantida['valores'] else 0,
            'idade_media_meses': np.mean(stats_nao_garantida['idades']) if stats_nao_garantida['idades'] else 0,
            'taxa_ajuizamento': stats_nao_garantida['ajuizados'] / stats_nao_garantida['total'] if stats_nao_garantida['total'] > 0 else 0
        }
    ])

    caminho_stats = output_dir / "analise_cnpj_mistos_stats.csv"
    stats_df.to_csv(caminho_stats, sep=";", index=False, encoding="utf-8-sig")
    print(f"[OK] Estatísticas salvas: {caminho_stats}")

# ============================================================================
# MAIN
# ============================================================================

def main():
    """
    Executa pipeline completo de análise econométrica.
    """
    print("=" * 80)
    print("ANÁLISE ECONOMÉTRICA DO SEGURO GARANTIA - BASE PGFN")
    print("=" * 80)
    print(f"\nData: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"Diretório de dados: {DATA_DIR}")

    # Verificar arquivo de entrada
    caminho_arquivo = DATA_DIR / ARQUIVO_ENTRADA

    if not caminho_arquivo.exists():
        print(f"\n[ERRO] Arquivo não encontrado: {caminho_arquivo}")
        print("\nVerificando arquivos disponíveis...")
        if DATA_DIR.exists():
            arquivos = list(DATA_DIR.glob("*.csv"))
            print(f"Arquivos CSV encontrados em {DATA_DIR}:")
            for arq in arquivos[:20]:
                print(f"  - {arq.name}")
        return

    # Criar diretório de saída
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Diretório de saída: {OUTPUT_DIR}")

    # =======================================================================
    # ETAPA 1 - DIAGNÓSTICO (OBRIGATÓRIO)
    # =======================================================================

    diagnostico = executar_diagnostico(caminho_arquivo, OUTPUT_DIR)

    if diagnostico is None:
        print("\n[ERRO] Falha no diagnóstico. Abortando.")
        return

    print("\n" + "=" * 80)
    print("DIAGNÓSTICO CONCLUÍDO")
    print("=" * 80)
    print("""
    IMPORTANTE: Revise o arquivo 'diagnostico_base_pgfn.txt' antes de prosseguir.

    Verifique especialmente os valores de SITUACAO_INSCRICAO para confirmar
    a definição correta da FLAG_GARANTIDA.

    Se necessário, ajuste a função definir_flag_garantida() no código.
    """)

    # Perguntar se deseja continuar
    print("\nProsseguindo automaticamente com as Etapas 2-4...")

    # =======================================================================
    # ETAPA 2 - VARIÁVEIS DERIVADAS
    # =======================================================================

    df_agg, agg_parcial = executar_etapa2(caminho_arquivo, diagnostico, OUTPUT_DIR)

    # =======================================================================
    # ETAPA 3 - CROSSTABS
    # =======================================================================

    caminho_enriquecido = OUTPUT_DIR / "pgfn_unificada_enriquecida_seguro_garantia.csv"
    executar_etapa3(caminho_enriquecido, OUTPUT_DIR, diagnostico)

    # =======================================================================
    # ETAPA 4 - ANÁLISE INTRA-CNPJ
    # =======================================================================

    executar_etapa4(caminho_enriquecido, df_agg, OUTPUT_DIR)

    # =======================================================================
    # RESUMO FINAL
    # =======================================================================

    print("\n" + "=" * 80)
    print("PIPELINE CONCLUÍDO")
    print("=" * 80)
    print(f"\nArquivos gerados em: {OUTPUT_DIR}")
    print("""
    Arquivos principais:
    • pgfn_unificada_enriquecida_seguro_garantia.csv - Base completa enriquecida
    • pgfn_agregado_cnpj_perfil_garantia.csv - Perfil agregado por CNPJ
    • diagnostico_base_pgfn.txt - Diagnóstico exploratório

    Crosstabs (pasta crosstabs_garantia/):
    • faixa_valor_x_garantida.csv
    • grupo_trib_x_garantida.csv
    • uf_x_garantida.csv
    • ano_x_garantida.csv
    • ajuizado_x_garantida.csv
    • tipo_garantia_x_ajuizado.csv

    Análise intra-CNPJ:
    • analise_cnpj_mistos.txt - Relatório comparativo
    • analise_cnpj_mistos_stats.csv - Estatísticas
    """)

if __name__ == "__main__":
    main()
