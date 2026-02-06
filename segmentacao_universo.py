#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline A — Segmentação do Universo Analisável
=================================================
Segmentação da base PGFN para isolar empresas elegíveis a seguro garantia judicial.

Etapas:
1. Mapeamento de colunas + CNPJ raiz
2. Exclusão Simples/MEI por CNPJ raiz
3. Segregação RJ/Falência + Mega-corporações
4. Base limpa + variáveis derivadas
5. Agregação por CNPJ raiz
6. Relatório de segmentação

Autor: Claude Code / Rodrigo
Data: Fevereiro 2026
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import argparse
import re
import gc
import time
import warnings
import os

warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

# Diretório de dados
DATA_DIR = Path(os.environ.get("PGFN_DATA_DIR", r"C:\Rodrigo\BasePGFN\2026"))

# Arquivo de entrada
ARQUIVO_ENTRADA = "pgfn_unificada_cnpj_principal_semibruta.csv"

# Diretório de saída
OUTPUT_DIR = DATA_DIR / "outputs_pipeline_a"

# Configurações de processamento
CHUNK_SIZE = 500_000
DATA_REFERENCIA = pd.Timestamp("2025-12-31")  # Série temporal encerra em 2025

# ============================================================================
# CONSTANTES DE NEGÓCIO
# ============================================================================

# Padrões para exclusão de Simples/MEI (case insensitive)
PADROES_SIMPLES_MEI = [
    r'simples\s+nacional',
    r'\bmei\b',
    r'microempreendedor',
    r'\bsimples\b',  # Captura "SIMPLES" isolado (regime antigo)
]

# Padrões para RJ/Falência (aplicar em NOME_DEVEDOR)
PADROES_RJ_FALENCIA = [
    (r'\bEM\s+RECUPERA[CÇ][AÃ]O\s+JUDICIAL\b', 'EM RECUPERACAO JUDICIAL'),
    (r'\bRECUPERA[CÇ][AÃ]O\s+JUDICIAL\b', 'RECUPERACAO JUDICIAL'),
    (r'\bEM\s+REC\.?\s*JUD\.?\b', 'EM REC JUD'),
    (r'\bMAS+A\s+FALIDA\b', 'MASSA FALIDA'),
    (r'\bMAS+A\s+FAL\b', 'MASSA FAL'),
    (r'\bEM\s+FAL[EÊ]NCIA\b', 'EM FALENCIA'),
    (r'\bFALIDA\b', 'FALIDA'),
    (r'\bEM\s+LIQUIDA[CÇ][AÃ]O\b', 'EM LIQUIDACAO'),
    (r'\bLIQUIDA[CÇ][AÃ]O\s+EXTRAJUDICIAL\b', 'LIQUIDACAO EXTRAJUDICIAL'),
    (r'\bLIQUIDA[CÇ][AÃ]O\s+JUDICIAL\b', 'LIQUIDACAO JUDICIAL'),
    (r'\b(?:EM\s+)?AUTOFALENCIA\b', 'AUTOFALENCIA'),
]

# Padrões para mega-corporações (aplicar em NOME_DEVEDOR)
MEGA_CORPS_NOMES = [
    (r'\bVALE\s+S[\./]?A\b', 'VALE'),
    (r'\bPETROBRAS\b', 'PETROBRAS'),
    (r'\bPETROLEO\s+BRASILEIRO\b', 'PETROBRAS'),
    (r'\bUSIMINAS\b', 'USIMINAS'),
    (r'\bBRASKEM\b', 'BRASKEM'),
    (r'\bSUZANO\b', 'SUZANO'),
    (r'\bITAU\s*UNIBANCO\b', 'ITAU'),
    (r'\bITAU\b', 'ITAU'),
    (r'\bBRADESCO\b', 'BRADESCO'),
    (r'\bSANTANDER\b', 'SANTANDER'),
    (r'\bCARREFOUR\b', 'CARREFOUR'),
    (r'\bATACADAO\b', 'ATACADAO'),
    (r'\bAMBEV\b', 'AMBEV'),
    (r'\bJBS\b', 'JBS'),
    (r'\bGERDAU\b', 'GERDAU'),
    (r'\bCSN\b', 'CSN'),
    (r'\bCOMPANHIA\s+SIDERURGICA\s+NACIONAL\b', 'CSN'),
    (r'\bEMBRAER\b', 'EMBRAER'),
    (r'\bRAIZEN\b', 'RAIZEN'),
    (r'\bCOSAN\b', 'COSAN'),
    (r'\bELETROBRAS\b', 'ELETROBRAS'),
    (r'\bCENTRAIS\s+ELETRICAS\b', 'ELETROBRAS'),
    (r'\bBANCO\s+DO\s+BRASIL\b', 'BANCO DO BRASIL'),
    (r'\bCAIXA\s+ECONOMICA\b', 'CAIXA'),
    (r'\bBNDES\b', 'BNDES'),
    (r'\bTELEFONICA\b', 'TELEFONICA'),
    (r'\bVIVO\b', 'VIVO'),
    (r'\bCLARO\b', 'CLARO'),
    (r'\bAMERICA\s+MOVIL\b', 'AMERICA MOVIL'),
    (r'\bTIM\b', 'TIM'),
    (r'\bOI\s+S[\./]?A\b', 'OI'),
    (r'\bRENNER\b', 'RENNER'),
    (r'\bMAGAZINE\s+LUIZA\b', 'MAGAZINE LUIZA'),
    (r'\bNATURA\b', 'NATURA'),
    (r'\bLOJAS\s+AMERICANAS\b', 'AMERICANAS'),
    (r'\bB3\s+S[\./]?A\b', 'B3'),
    (r'\bWEG\b', 'WEG'),
    (r'\bLOCAWEB\b', 'LOCAWEB'),
    (r'\bTOTVS\b', 'TOTVS'),
]

# Mapeamento de RECEITA_PRINCIPAL para GRUPO_TRIBUTARIO
# Ordem de avaliação importa! Mais específico primeiro.
GRUPOS_TRIBUTARIOS = {
    'SIMPLES_MEI': ['simples nacional', 'mei', 'microempreendedor', 'simples'],
    'IPI': ['ipi'],
    'IOF': ['iof'],
    'ITR': ['itr', 'rural'],
    'IMPORTACAO': ['importação', 'importa'],
    'IRRF': ['irrf'],
    'IRPJ_CSLL': ['irpj', 'csll'],
    'COFINS_PIS': ['cofins', 'pis', 'pasep'],
    'CIDE_SEBRAE': ['cide', 'sebrae', 'apex', 'abdi'],
    'TERCEIROS': ['terceiros', 'sesc', 'senac', 'sesi', 'senai', 'sest', 'senat',
                  'senar', 'sescoop', 'incra', 'educação', 'salário educação'],
    'PREVIDENCIARIO': ['previdenciária', 'segurados', 'empregador', 'empresa/empregador',
                       'risco ambiental', 'aposent', 'receita bruta', 'contribuição'],
    'MULTAS': ['multa'],
    'FGTS': ['fgts'],
    'SPU': ['spu'],
}

# Faixas de valor
FAIXAS_VALOR = [0, 10_000, 50_000, 100_000, 500_000, 2_000_000, 10_000_000, 50_000_000, float('inf')]
LABELS_FAIXAS = ['Ate_10k', '10k_50k', '50k_100k', '100k_500k', '500k_2M', '2M_10M', '10M_50M', 'Acima_50M']

# ============================================================================
# FUNÇÕES DE FORMATAÇÃO (PADRÃO BRASILEIRO)
# ============================================================================

def fmt_brl(valor):
    """Formata valor numérico para padrão brasileiro R$ 1.234.567,89"""
    if pd.isna(valor) or valor == 0:
        return 'R$ 0,00'
    s = f'{valor:,.2f}'
    s = s.replace(',', 'X').replace('.', ',').replace('X', '.')
    return f'R$ {s}'

def fmt_pct(valor):
    """Formata valor decimal (0.1234) para '12,34%'"""
    if pd.isna(valor):
        return '0,00%'
    s = f'{valor * 100:.2f}'.replace('.', ',')
    return f'{s}%'

def fmt_num(valor, casas=0):
    """Formata número com separador de milhar brasileiro"""
    if pd.isna(valor):
        return '0'
    if casas == 0:
        s = f'{valor:,.0f}'
    else:
        s = f'{valor:,.{casas}f}'
    s = s.replace(',', 'X').replace('.', ',').replace('X', '.')
    return s

# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def limpar_cnpj(cnpj):
    """Remove formatação do CNPJ, mantendo apenas dígitos."""
    if pd.isna(cnpj):
        return ""
    return re.sub(r'[^\d]', '', str(cnpj))

def extrair_cnpj_raiz(cnpj_limpo):
    """Extrai os 8 primeiros dígitos do CNPJ (raiz)."""
    if len(cnpj_limpo) >= 8:
        return cnpj_limpo[:8]
    return cnpj_limpo

def extrair_cnpj_filial(cnpj_limpo):
    """Extrai os dígitos 9-12 do CNPJ (filial)."""
    if len(cnpj_limpo) >= 12:
        return cnpj_limpo[8:12]
    return ""

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

def classificar_grupo_tributario(receita):
    """Classifica RECEITA_PRINCIPAL em grupo tributário."""
    if pd.isna(receita):
        return 'OUTROS'
    receita_lower = str(receita).lower()

    # Ordem importa - mais específico primeiro
    for grupo, termos in GRUPOS_TRIBUTARIOS.items():
        for termo in termos:
            if termo in receita_lower:
                return grupo
    return 'OUTROS'

def eh_simples_mei(receita):
    """Verifica se a receita é Simples/MEI."""
    if pd.isna(receita):
        return False
    receita_lower = str(receita).lower()
    for padrao in PADROES_SIMPLES_MEI:
        if re.search(padrao, receita_lower, re.IGNORECASE):
            return True
    return False

def detectar_rj_falencia(nome):
    """Detecta padrões de RJ/Falência no nome. Retorna (bool, termo_detectado)."""
    if pd.isna(nome):
        return False, None
    nome_upper = str(nome).upper()
    for padrao, termo in PADROES_RJ_FALENCIA:
        if re.search(padrao, nome_upper, re.IGNORECASE):
            return True, termo
    return False, None

def detectar_mega_corp(nome):
    """Detecta se é mega-corporação. Retorna (bool, nome_empresa)."""
    if pd.isna(nome):
        return False, None
    nome_upper = str(nome).upper()
    for padrao, empresa in MEGA_CORPS_NOMES:
        if re.search(padrao, nome_upper, re.IGNORECASE):
            return True, empresa
    return False, None

def classificar_garantia(tipo_situacao, situacao_inscricao):
    """Classifica tipo de garantia. Retorna (flag_garantida, tipo_garantia)."""
    if pd.isna(tipo_situacao):
        return 0, 'SEM_GARANTIA'

    tipo_upper = str(tipo_situacao).upper().strip()

    if 'GARANTIA' not in tipo_upper:
        return 0, 'SEM_GARANTIA'

    # Detalhar tipo via SITUACAO_INSCRICAO
    sit = str(situacao_inscricao).upper() if not pd.isna(situacao_inscricao) else ''

    if 'SEGURO GARANTIA' in sit:
        return 1, 'SEGURO_GARANTIA'
    elif 'CARTA FIANCA' in sit or 'CARTA FIANÇA' in sit:
        return 1, 'CARTA_FIANCA'
    elif 'DEPOSITO JUDICIAL' in sit or 'DEPÓSITO JUDICIAL' in sit:
        return 1, 'DEPOSITO_JUDICIAL'
    elif 'GARANTIA - DEPOSITO' in sit or 'GARANTIA - DEPÓSITO' in sit:
        return 1, 'DEPOSITO_EXECUCAO'
    elif 'GARANTIA - PENHORA' in sit:
        return 1, 'PENHORA'
    else:
        return 1, 'OUTRA_GARANTIA'

def calcular_proxy_porte(grupos_trib_set, total_inscricoes, valor_total):
    """Calcula proxy de porte baseado no mix tributário."""
    tem_irpj = 'IRPJ_CSLL' in grupos_trib_set
    tem_cofins = 'COFINS_PIS' in grupos_trib_set
    tem_ipi = 'IPI' in grupos_trib_set
    tem_iof = 'IOF' in grupos_trib_set
    tem_import = 'IMPORTACAO' in grupos_trib_set

    # Alto: combinação de tributos federais complexos
    if (tem_irpj and tem_cofins) or tem_ipi or tem_iof or tem_import:
        return 'ALTO'
    # Médio: tem IRPJ/CSLL ou volume significativo
    elif tem_irpj or tem_cofins or total_inscricoes > 20 or valor_total > 5_000_000:
        return 'MEDIO'
    else:
        return 'BAIXO'

# ============================================================================
# ETAPA 1: MAPEAMENTO DE COLUNAS + CNPJ RAIZ
# ============================================================================

def etapa1_mapeamento(caminho_arquivo, output_dir, modo_teste=False):
    """
    Etapa 1: Detecta colunas, trata BOM, cria mapeamento CNPJ → CNPJ raiz.
    """
    print("\n" + "=" * 80)
    print("ETAPA 1 - MAPEAMENTO DE COLUNAS + CNPJ RAIZ")
    print("=" * 80)

    inicio = time.time()

    # Verificar arquivo
    if not caminho_arquivo.exists():
        print(f"[ERRO] Arquivo não encontrado: {caminho_arquivo}")
        return None

    # Ler amostra para detectar formato
    print("\n1.1 - Detectando formato do arquivo...")

    # Tentar utf-8-sig primeiro (consome BOM automaticamente)
    try:
        amostra = pd.read_csv(caminho_arquivo, sep=';', encoding='utf-8-sig',
                              nrows=5, dtype=str)
        encoding = 'utf-8-sig'
        print(f"    Encoding: utf-8-sig (BOM consumido automaticamente)")
    except:
        # Fallback para latin-1
        amostra = pd.read_csv(caminho_arquivo, sep=';', encoding='latin-1',
                              nrows=5, dtype=str)
        encoding = 'latin-1'
        # Remover BOM manualmente
        amostra.columns = [c.replace('\ufeff', '').replace('ï»¿', '').strip()
                           for c in amostra.columns]
        print(f"    Encoding: latin-1 (BOM removido manualmente)")

    print(f"    Separador: ;")
    print(f"    Colunas detectadas: {len(amostra.columns)}")

    # Mostrar colunas
    print("\n1.2 - Colunas do arquivo:")
    for i, col in enumerate(amostra.columns, 1):
        print(f"    {i:2}. {col}")

    # Identificar coluna do CNPJ
    col_cnpj = None
    for col in amostra.columns:
        if 'CPF' in col.upper() or 'CNPJ' in col.upper():
            col_cnpj = col
            break

    if not col_cnpj:
        print("[ERRO] Coluna de CNPJ não encontrada!")
        return None

    print(f"\n    Coluna CNPJ identificada: {col_cnpj}")

    # Criar diretório de saída
    output_dir.mkdir(parents=True, exist_ok=True)

    # Processar CNPJ raiz
    print("\n1.3 - Criando mapeamento CNPJ → CNPJ raiz...")

    nrows = 100_000 if modo_teste else None

    # Dicionário para acumular: CNPJ_RAIZ → set de CNPJs completos
    mapa_cnpj_raiz = {}

    # Contadores
    total_linhas = 0
    cnpj_invalidos = 0
    chunks_processados = 0

    for chunk in pd.read_csv(
        caminho_arquivo,
        sep=';',
        encoding=encoding,
        dtype=str,
        chunksize=CHUNK_SIZE,
        nrows=nrows,
        low_memory=False
    ):
        chunks_processados += 1

        # Tratar BOM se encoding latin-1
        if encoding == 'latin-1' and chunks_processados == 1:
            chunk.columns = [c.replace('\ufeff', '').replace('ï»¿', '').strip()
                             for c in chunk.columns]
            col_cnpj = [c for c in chunk.columns if 'CPF' in c.upper() or 'CNPJ' in c.upper()][0]

        total_linhas += len(chunk)

        for cnpj in chunk[col_cnpj]:
            cnpj_limpo = limpar_cnpj(cnpj)
            if len(cnpj_limpo) < 14:
                cnpj_invalidos += 1
                continue

            cnpj_raiz = extrair_cnpj_raiz(cnpj_limpo)

            if cnpj_raiz not in mapa_cnpj_raiz:
                mapa_cnpj_raiz[cnpj_raiz] = set()
            mapa_cnpj_raiz[cnpj_raiz].add(cnpj_limpo)

        if chunks_processados % 5 == 0:
            print(f"    Chunk {chunks_processados}: {total_linhas:,} linhas | "
                  f"{len(mapa_cnpj_raiz):,} CNPJs raiz")
            gc.collect()

    # Criar DataFrame do mapeamento
    print("\n1.4 - Gerando arquivo de mapeamento...")

    rows = []
    for cnpj_raiz, cnpjs in mapa_cnpj_raiz.items():
        for cnpj in cnpjs:
            rows.append({
                'CPF_CNPJ': cnpj,
                'CNPJ_RAIZ': cnpj_raiz,
                'CNPJ_FILIAL': extrair_cnpj_filial(cnpj),
                'FLAG_MATRIZ': 1 if extrair_cnpj_filial(cnpj) == '0001' else 0
            })

    df_mapa = pd.DataFrame(rows)
    df_mapa = df_mapa.sort_values(['CNPJ_RAIZ', 'CPF_CNPJ'])

    # Contar filiais por CNPJ raiz
    filiais_por_raiz = df_mapa.groupby('CNPJ_RAIZ').size().reset_index(name='QTD_FILIAIS')

    # Salvar mapeamento
    caminho_mapa = output_dir / "pgfn_mapa_cnpj_raiz.csv"
    df_mapa.to_csv(caminho_mapa, sep=';', index=False, encoding='utf-8-sig')

    # Estatísticas
    total_cnpjs = len(df_mapa)
    total_raiz = len(mapa_cnpj_raiz)
    multi_filial = (filiais_por_raiz['QTD_FILIAIS'] > 1).sum()
    max_filiais = filiais_por_raiz['QTD_FILIAIS'].max()
    cnpj_max_filiais = filiais_por_raiz.loc[filiais_por_raiz['QTD_FILIAIS'].idxmax(), 'CNPJ_RAIZ']

    tempo = time.time() - inicio

    print(f"\n[OK] Mapeamento salvo: {caminho_mapa}")
    print(f"\n1.5 - Estatísticas:")
    print(f"    Total de linhas processadas: {fmt_num(total_linhas)}")
    print(f"    Total CNPJs completos: {fmt_num(total_cnpjs)}")
    print(f"    Total CNPJs raiz: {fmt_num(total_raiz)}")
    print(f"    Empresas multi-filial: {fmt_num(multi_filial)} ({fmt_pct(multi_filial/total_raiz)})")
    print(f"    Máximo filiais por CNPJ raiz: {max_filiais} (CNPJ raiz: {cnpj_max_filiais})")
    print(f"    CNPJs inválidos (<14 dígitos): {fmt_num(cnpj_invalidos)}")
    print(f"\n    Tempo de execução: {tempo:.1f}s")

    return {
        'encoding': encoding,
        'col_cnpj': col_cnpj,
        'total_linhas': total_linhas,
        'total_cnpjs': total_cnpjs,
        'total_raiz': total_raiz,
        'multi_filial': multi_filial,
        'max_filiais': max_filiais,
        'cnpj_invalidos': cnpj_invalidos,
        'mapa_cnpj_raiz': mapa_cnpj_raiz,
        'tempo': tempo
    }

# ============================================================================
# ETAPA 2: EXCLUSÃO SIMPLES/MEI POR CNPJ RAIZ
# ============================================================================

def etapa2_exclusao_simples_mei(caminho_arquivo, output_dir, info_etapa1, modo_teste=False):
    """
    Etapa 2: Identifica CNPJs raiz com qualquer inscrição Simples/MEI e exclui TODAS
    as inscrições desse grupo econômico.
    """
    print("\n" + "=" * 80)
    print("ETAPA 2 - EXCLUSÃO SIMPLES NACIONAL / MEI")
    print("=" * 80)

    inicio = time.time()

    encoding = info_etapa1['encoding']
    col_cnpj = info_etapa1['col_cnpj']

    nrows = 100_000 if modo_teste else None

    # FASE 1: Identificar CNPJs raiz com Simples/MEI
    print("\n2.1 - Identificando CNPJs raiz com Simples/MEI...")

    cnpjs_raiz_simples = set()
    chunks_processados = 0
    total_inscricoes_simples_diretas = 0

    for chunk in pd.read_csv(
        caminho_arquivo,
        sep=';',
        encoding=encoding,
        dtype=str,
        chunksize=CHUNK_SIZE,
        nrows=nrows,
        low_memory=False
    ):
        chunks_processados += 1

        if encoding == 'latin-1' and chunks_processados == 1:
            chunk.columns = [c.replace('\ufeff', '').replace('ï»¿', '').strip()
                             for c in chunk.columns]

        # Limpar CNPJ e extrair raiz
        chunk['_CNPJ_LIMPO'] = chunk[col_cnpj].apply(limpar_cnpj)
        chunk['_CNPJ_RAIZ'] = chunk['_CNPJ_LIMPO'].apply(extrair_cnpj_raiz)

        # Verificar Simples/MEI
        if 'RECEITA_PRINCIPAL' in chunk.columns:
            mask_simples = chunk['RECEITA_PRINCIPAL'].apply(eh_simples_mei)
            total_inscricoes_simples_diretas += mask_simples.sum()
            cnpjs_raiz_simples.update(chunk.loc[mask_simples, '_CNPJ_RAIZ'].unique())

        if chunks_processados % 10 == 0:
            print(f"    Chunk {chunks_processados}: {len(cnpjs_raiz_simples):,} CNPJs raiz com Simples/MEI")
            gc.collect()

    print(f"\n    CNPJs raiz com Simples/MEI: {fmt_num(len(cnpjs_raiz_simples))}")
    print(f"    Inscrições Simples/MEI diretas: {fmt_num(total_inscricoes_simples_diretas)}")

    # FASE 2: Segregar inscrições
    print("\n2.2 - Segregando inscrições...")

    # Arquivos de saída
    caminho_excluidos = output_dir / "pgfn_simples_mei_excluidos.csv"
    caminho_restante = output_dir / "_temp_restante_pos_simples.csv"

    primeiro_chunk_excl = True
    primeiro_chunk_rest = True
    total_excluidas = 0
    total_restantes = 0
    valor_excluido = 0.0
    chunks_processados = 0

    for chunk in pd.read_csv(
        caminho_arquivo,
        sep=';',
        encoding=encoding,
        dtype=str,
        chunksize=CHUNK_SIZE,
        nrows=nrows,
        low_memory=False
    ):
        chunks_processados += 1

        if encoding == 'latin-1' and chunks_processados == 1:
            chunk.columns = [c.replace('\ufeff', '').replace('ï»¿', '').strip()
                             for c in chunk.columns]

        # Adicionar colunas derivadas
        chunk['_CNPJ_LIMPO'] = chunk[col_cnpj].apply(limpar_cnpj)
        chunk['_CNPJ_RAIZ'] = chunk['_CNPJ_LIMPO'].apply(extrair_cnpj_raiz)
        chunk['CPF_CNPJ'] = chunk['_CNPJ_LIMPO']  # Padronizar nome

        # Converter valor
        if 'VALOR_CONSOLIDADO' in chunk.columns:
            chunk['VALOR_CONSOLIDADO_NUM'] = chunk['VALOR_CONSOLIDADO'].apply(converter_valor_brl)
        else:
            chunk['VALOR_CONSOLIDADO_NUM'] = 0.0

        # Separar excluídos e restantes
        mask_excluir = chunk['_CNPJ_RAIZ'].isin(cnpjs_raiz_simples)

        df_excluidos = chunk[mask_excluir].copy()
        df_restantes = chunk[~mask_excluir].copy()

        total_excluidas += len(df_excluidos)
        total_restantes += len(df_restantes)
        valor_excluido += df_excluidos['VALOR_CONSOLIDADO_NUM'].sum()

        # Salvar excluídos
        if len(df_excluidos) > 0:
            if primeiro_chunk_excl:
                df_excluidos.to_csv(caminho_excluidos, sep=';', index=False,
                                    encoding='utf-8-sig', mode='w')
                primeiro_chunk_excl = False
            else:
                df_excluidos.to_csv(caminho_excluidos, sep=';', index=False,
                                    encoding='utf-8-sig', mode='a', header=False)

        # Salvar restantes (temporário)
        if len(df_restantes) > 0:
            if primeiro_chunk_rest:
                df_restantes.to_csv(caminho_restante, sep=';', index=False,
                                    encoding='utf-8-sig', mode='w')
                primeiro_chunk_rest = False
            else:
                df_restantes.to_csv(caminho_restante, sep=';', index=False,
                                    encoding='utf-8-sig', mode='a', header=False)

        if chunks_processados % 5 == 0:
            print(f"    Chunk {chunks_processados}: excluídas {total_excluidas:,} | restantes {total_restantes:,}")
            gc.collect()

    tempo = time.time() - inicio

    print(f"\n[OK] Arquivo de excluídos salvo: {caminho_excluidos}")
    print(f"\n2.3 - Estatísticas:")
    print(f"    CNPJs raiz excluídos: {fmt_num(len(cnpjs_raiz_simples))}")
    print(f"    Inscrições excluídas: {fmt_num(total_excluidas)}")
    print(f"    Valor excluído: {fmt_brl(valor_excluido)}")
    print(f"    Inscrições restantes: {fmt_num(total_restantes)}")
    print(f"\n    Tempo de execução: {tempo:.1f}s")

    return {
        'cnpjs_raiz_simples': cnpjs_raiz_simples,
        'total_excluidas': total_excluidas,
        'total_restantes': total_restantes,
        'valor_excluido': valor_excluido,
        'caminho_restante': caminho_restante,
        'tempo': tempo
    }

# ============================================================================
# ETAPA 3: SEGREGAÇÃO RJ/FALÊNCIA + MEGA-CORPORAÇÕES
# ============================================================================

def etapa3_segregacao_rj_megacorps(output_dir, info_etapa2, modo_teste=False):
    """
    Etapa 3: Segrega CNPJs em RJ/Falência e mega-corporações.
    """
    print("\n" + "=" * 80)
    print("ETAPA 3 - SEGREGAÇÃO RJ/FALÊNCIA + MEGA-CORPORAÇÕES")
    print("=" * 80)

    inicio = time.time()

    caminho_entrada = info_etapa2['caminho_restante']

    # FASE 1: Identificar CNPJs raiz com RJ/Falência
    print("\n3.1 - Identificando CNPJs raiz em RJ/Falência...")

    cnpjs_raiz_rj = {}  # CNPJ_RAIZ → termo detectado
    log_rj = []
    chunks_processados = 0

    for chunk in pd.read_csv(
        caminho_entrada,
        sep=';',
        encoding='utf-8-sig',
        dtype=str,
        chunksize=CHUNK_SIZE,
        low_memory=False
    ):
        chunks_processados += 1

        if 'NOME_DEVEDOR' in chunk.columns:
            for _, row in chunk.iterrows():
                nome = row.get('NOME_DEVEDOR', '')
                cnpj_raiz = row.get('_CNPJ_RAIZ', '')

                if cnpj_raiz and cnpj_raiz not in cnpjs_raiz_rj:
                    eh_rj, termo = detectar_rj_falencia(nome)
                    if eh_rj:
                        cnpjs_raiz_rj[cnpj_raiz] = termo
                        log_rj.append({
                            'CNPJ_RAIZ': cnpj_raiz,
                            'CPF_CNPJ': row.get('CPF_CNPJ', ''),
                            'NOME_DEVEDOR': nome,
                            'PADRAO_DETECTADO': termo
                        })

        if chunks_processados % 10 == 0:
            print(f"    Chunk {chunks_processados}: {len(cnpjs_raiz_rj):,} CNPJs raiz em RJ/Falência")
            gc.collect()

    print(f"\n    CNPJs raiz em RJ/Falência: {fmt_num(len(cnpjs_raiz_rj))}")

    # Salvar log RJ
    if log_rj:
        df_log_rj = pd.DataFrame(log_rj)
        df_log_rj.to_csv(output_dir / "log_rj_falencia_termos_detectados.csv",
                         sep=';', index=False, encoding='utf-8-sig')

    # FASE 2: Identificar mega-corporações por nome
    print("\n3.2 - Identificando mega-corporações por nome...")

    cnpjs_raiz_megacorp = {}  # CNPJ_RAIZ → nome empresa
    log_megacorp = []
    chunks_processados = 0

    for chunk in pd.read_csv(
        caminho_entrada,
        sep=';',
        encoding='utf-8-sig',
        dtype=str,
        chunksize=CHUNK_SIZE,
        low_memory=False
    ):
        chunks_processados += 1

        if 'NOME_DEVEDOR' in chunk.columns:
            for _, row in chunk.iterrows():
                nome = row.get('NOME_DEVEDOR', '')
                cnpj_raiz = row.get('_CNPJ_RAIZ', '')

                # Não incluir se já está em RJ
                if cnpj_raiz and cnpj_raiz not in cnpjs_raiz_rj and cnpj_raiz not in cnpjs_raiz_megacorp:
                    eh_mega, empresa = detectar_mega_corp(nome)
                    if eh_mega:
                        cnpjs_raiz_megacorp[cnpj_raiz] = empresa
                        log_megacorp.append({
                            'CNPJ_RAIZ': cnpj_raiz,
                            'CPF_CNPJ': row.get('CPF_CNPJ', ''),
                            'NOME_DEVEDOR': nome,
                            'EMPRESA_IDENTIFICADA': empresa,
                            'CRITERIO': 'NOME'
                        })

        if chunks_processados % 10 == 0:
            gc.collect()

    print(f"    Mega-corps por nome: {fmt_num(len(cnpjs_raiz_megacorp))}")

    # FASE 3: Identificar mega-corps por valor (P99.5)
    print("\n3.3 - Identificando mega-corporações por valor (P99.5)...")

    # Primeiro, calcular valores por CNPJ raiz (excluindo RJ e mega-corps já identificadas)
    valores_por_raiz = {}
    chunks_processados = 0

    for chunk in pd.read_csv(
        caminho_entrada,
        sep=';',
        encoding='utf-8-sig',
        dtype=str,
        chunksize=CHUNK_SIZE,
        low_memory=False
    ):
        chunks_processados += 1

        chunk['VALOR_CONSOLIDADO_NUM'] = pd.to_numeric(chunk['VALOR_CONSOLIDADO_NUM'], errors='coerce').fillna(0)

        for cnpj_raiz, grupo in chunk.groupby('_CNPJ_RAIZ'):
            if cnpj_raiz in cnpjs_raiz_rj or cnpj_raiz in cnpjs_raiz_megacorp:
                continue
            if cnpj_raiz not in valores_por_raiz:
                valores_por_raiz[cnpj_raiz] = {'valor': 0.0, 'nome': None}
            valores_por_raiz[cnpj_raiz]['valor'] += grupo['VALOR_CONSOLIDADO_NUM'].sum()
            if valores_por_raiz[cnpj_raiz]['nome'] is None and 'NOME_DEVEDOR' in grupo.columns:
                valores_por_raiz[cnpj_raiz]['nome'] = grupo['NOME_DEVEDOR'].iloc[0]

        gc.collect()

    # Calcular P99.5
    valores = [v['valor'] for v in valores_por_raiz.values()]
    if valores:
        p995 = np.percentile(valores, 99.5)
        print(f"    P99.5 do valor por CNPJ raiz: {fmt_brl(p995)}")

        # Identificar CNPJs acima do P99.5
        for cnpj_raiz, info in valores_por_raiz.items():
            if info['valor'] >= p995 and cnpj_raiz not in cnpjs_raiz_megacorp:
                cnpjs_raiz_megacorp[cnpj_raiz] = 'P99.5_VALOR'
                log_megacorp.append({
                    'CNPJ_RAIZ': cnpj_raiz,
                    'CPF_CNPJ': '',
                    'NOME_DEVEDOR': info['nome'] or '',
                    'EMPRESA_IDENTIFICADA': 'P99.5_VALOR',
                    'CRITERIO': 'VALOR',
                    'VALOR_TOTAL': info['valor']
                })

    print(f"    Total mega-corps (nome + valor): {fmt_num(len(cnpjs_raiz_megacorp))}")

    # Salvar log mega-corps
    if log_megacorp:
        df_log_mega = pd.DataFrame(log_megacorp)
        df_log_mega.to_csv(output_dir / "log_mega_corps_identificadas.csv",
                           sep=';', index=False, encoding='utf-8-sig')

    # FASE 4: Segregar arquivos
    print("\n3.4 - Segregando arquivos...")

    caminho_rj = output_dir / "pgfn_rj_falencia_sinistralidade.csv"
    caminho_mega = output_dir / "pgfn_mega_corps_segregadas.csv"
    caminho_restante = output_dir / "_temp_restante_pos_segregacao.csv"

    primeiro_rj = True
    primeiro_mega = True
    primeiro_rest = True
    total_rj = 0
    total_mega = 0
    total_rest = 0
    valor_rj = 0.0
    valor_mega = 0.0
    chunks_processados = 0

    for chunk in pd.read_csv(
        caminho_entrada,
        sep=';',
        encoding='utf-8-sig',
        dtype=str,
        chunksize=CHUNK_SIZE,
        low_memory=False
    ):
        chunks_processados += 1

        chunk['VALOR_CONSOLIDADO_NUM'] = pd.to_numeric(chunk['VALOR_CONSOLIDADO_NUM'], errors='coerce').fillna(0)

        # Separar por categoria
        mask_rj = chunk['_CNPJ_RAIZ'].isin(cnpjs_raiz_rj.keys())
        mask_mega = chunk['_CNPJ_RAIZ'].isin(cnpjs_raiz_megacorp.keys()) & ~mask_rj
        mask_rest = ~mask_rj & ~mask_mega

        df_rj = chunk[mask_rj].copy()
        df_mega = chunk[mask_mega].copy()
        df_rest = chunk[mask_rest].copy()

        total_rj += len(df_rj)
        total_mega += len(df_mega)
        total_rest += len(df_rest)
        valor_rj += df_rj['VALOR_CONSOLIDADO_NUM'].sum()
        valor_mega += df_mega['VALOR_CONSOLIDADO_NUM'].sum()

        # Salvar RJ
        if len(df_rj) > 0:
            if primeiro_rj:
                df_rj.to_csv(caminho_rj, sep=';', index=False, encoding='utf-8-sig', mode='w')
                primeiro_rj = False
            else:
                df_rj.to_csv(caminho_rj, sep=';', index=False, encoding='utf-8-sig', mode='a', header=False)

        # Salvar mega-corps
        if len(df_mega) > 0:
            if primeiro_mega:
                df_mega.to_csv(caminho_mega, sep=';', index=False, encoding='utf-8-sig', mode='w')
                primeiro_mega = False
            else:
                df_mega.to_csv(caminho_mega, sep=';', index=False, encoding='utf-8-sig', mode='a', header=False)

        # Salvar restante
        if len(df_rest) > 0:
            if primeiro_rest:
                df_rest.to_csv(caminho_restante, sep=';', index=False, encoding='utf-8-sig', mode='w')
                primeiro_rest = False
            else:
                df_rest.to_csv(caminho_restante, sep=';', index=False, encoding='utf-8-sig', mode='a', header=False)

        if chunks_processados % 5 == 0:
            print(f"    Chunk {chunks_processados}: RJ {total_rj:,} | Mega {total_mega:,} | Rest {total_rest:,}")
            gc.collect()

    # Limpar arquivo temporário da etapa 2
    if info_etapa2['caminho_restante'].exists():
        info_etapa2['caminho_restante'].unlink()

    tempo = time.time() - inicio

    print(f"\n[OK] Arquivos salvos:")
    print(f"    - {caminho_rj}")
    print(f"    - {caminho_mega}")

    print(f"\n3.5 - Estatísticas:")
    print(f"    RJ/Falência:")
    print(f"      CNPJs raiz: {fmt_num(len(cnpjs_raiz_rj))}")
    print(f"      Inscrições: {fmt_num(total_rj)}")
    print(f"      Valor: {fmt_brl(valor_rj)}")
    print(f"\n    Mega-corporações:")
    print(f"      CNPJs raiz: {fmt_num(len(cnpjs_raiz_megacorp))}")
    print(f"      Inscrições: {fmt_num(total_mega)}")
    print(f"      Valor: {fmt_brl(valor_mega)}")
    print(f"\n    Restante para universo analisável: {fmt_num(total_rest)}")
    print(f"\n    Tempo de execução: {tempo:.1f}s")

    return {
        'cnpjs_raiz_rj': cnpjs_raiz_rj,
        'cnpjs_raiz_megacorp': cnpjs_raiz_megacorp,
        'total_rj': total_rj,
        'total_mega': total_mega,
        'total_restante': total_rest,
        'valor_rj': valor_rj,
        'valor_mega': valor_mega,
        'caminho_restante': caminho_restante,
        'tempo': tempo
    }

# ============================================================================
# ETAPA 4: BASE LIMPA + VARIÁVEIS DERIVADAS
# ============================================================================

def etapa4_base_limpa(output_dir, info_etapa3, modo_teste=False):
    """
    Etapa 4: Gera base limpa com todas as variáveis derivadas.
    """
    print("\n" + "=" * 80)
    print("ETAPA 4 - BASE LIMPA + VARIÁVEIS DERIVADAS")
    print("=" * 80)

    inicio = time.time()

    caminho_entrada = info_etapa3['caminho_restante']
    caminho_saida = output_dir / "pgfn_universo_analisavel.csv"

    print(f"\n4.1 - Processando base limpa com variáveis derivadas...")

    primeiro_chunk = True
    total_linhas = 0
    erros_data = 0
    erros_valor = 0
    chunks_processados = 0

    for chunk in pd.read_csv(
        caminho_entrada,
        sep=';',
        encoding='utf-8-sig',
        dtype=str,
        chunksize=CHUNK_SIZE,
        low_memory=False
    ):
        chunks_processados += 1
        total_linhas += len(chunk)

        # --- Variáveis de CNPJ ---
        chunk['CNPJ_RAIZ'] = chunk['_CNPJ_RAIZ']
        chunk['CNPJ_FILIAL'] = chunk['_CNPJ_LIMPO'].apply(extrair_cnpj_filial)
        chunk['FLAG_MATRIZ'] = (chunk['CNPJ_FILIAL'] == '0001').astype('int8')

        # --- Data ---
        chunk['DATA_INSCRICAO_DT'] = pd.to_datetime(chunk['DATA_INSCRICAO'], errors='coerce')
        erros_data += chunk['DATA_INSCRICAO_DT'].isna().sum()

        chunk['ANO_INSCRICAO'] = chunk['DATA_INSCRICAO_DT'].dt.year
        chunk['MES_INSCRICAO'] = chunk['DATA_INSCRICAO_DT'].dt.month
        chunk['IDADE_INSCRICAO_MESES'] = (
            (DATA_REFERENCIA - chunk['DATA_INSCRICAO_DT']).dt.days / 30.44
        ).round(0)

        # --- Valor ---
        chunk['VALOR_CONSOLIDADO_NUM'] = pd.to_numeric(chunk['VALOR_CONSOLIDADO_NUM'], errors='coerce')
        erros_valor += chunk['VALOR_CONSOLIDADO_NUM'].isna().sum()
        chunk['VALOR_CONSOLIDADO_NUM'] = chunk['VALOR_CONSOLIDADO_NUM'].fillna(0)

        chunk['LOG_VALOR'] = np.log1p(chunk['VALOR_CONSOLIDADO_NUM'])

        chunk['FAIXA_VALOR'] = pd.cut(
            chunk['VALOR_CONSOLIDADO_NUM'],
            bins=FAIXAS_VALOR,
            labels=LABELS_FAIXAS,
            right=True
        )

        # --- Garantia ---
        garantias = chunk.apply(
            lambda row: classificar_garantia(
                row.get('TIPO_SITUACAO_INSCRICAO', ''),
                row.get('SITUACAO_INSCRICAO', '')
            ), axis=1
        )
        chunk['FLAG_GARANTIDA'] = [g[0] for g in garantias]
        chunk['TIPO_GARANTIA'] = [g[1] for g in garantias]

        # --- Ajuizado ---
        if 'INDICADOR_AJUIZADO' in chunk.columns:
            chunk['FLAG_AJUIZADO'] = (
                chunk['INDICADOR_AJUIZADO'].fillna('').str.upper().str.strip() == 'SIM'
            ).astype('int8')
        else:
            chunk['FLAG_AJUIZADO'] = 0

        # --- Grupo tributário ---
        chunk['GRUPO_TRIBUTARIO'] = chunk['RECEITA_PRINCIPAL'].apply(classificar_grupo_tributario)

        # --- Formatar valores para visualização ---
        chunk['VALOR_CONSOLIDADO_FMT'] = chunk['VALOR_CONSOLIDADO_NUM'].apply(fmt_brl)

        # Remover colunas temporárias
        cols_remover = ['_CNPJ_LIMPO', '_CNPJ_RAIZ']
        chunk = chunk.drop(columns=[c for c in cols_remover if c in chunk.columns], errors='ignore')

        # Salvar
        if primeiro_chunk:
            chunk.to_csv(caminho_saida, sep=';', index=False, encoding='utf-8-sig', mode='w')
            primeiro_chunk = False
        else:
            chunk.to_csv(caminho_saida, sep=';', index=False, encoding='utf-8-sig', mode='a', header=False)

        if chunks_processados % 5 == 0:
            print(f"    Chunk {chunks_processados}: {total_linhas:,} linhas processadas")
            gc.collect()

    # Limpar arquivo temporário
    if caminho_entrada.exists():
        caminho_entrada.unlink()

    tempo = time.time() - inicio

    print(f"\n[OK] Base limpa salva: {caminho_saida}")
    print(f"\n4.2 - Estatísticas:")
    print(f"    Total de linhas: {fmt_num(total_linhas)}")
    print(f"    Erros de data: {fmt_num(erros_data)}")
    print(f"    Erros de valor: {fmt_num(erros_valor)}")
    print(f"\n    Tempo de execução: {tempo:.1f}s")

    return {
        'total_linhas': total_linhas,
        'erros_data': erros_data,
        'erros_valor': erros_valor,
        'caminho_saida': caminho_saida,
        'tempo': tempo
    }

# ============================================================================
# ETAPA 5: AGREGAÇÃO POR CNPJ RAIZ
# ============================================================================

def etapa5_agregacao_cnpj_raiz(output_dir, info_etapa4, modo_teste=False):
    """
    Etapa 5: Agrega dados por CNPJ raiz com todas as métricas.
    """
    print("\n" + "=" * 80)
    print("ETAPA 5 - AGREGAÇÃO POR CNPJ RAIZ")
    print("=" * 80)

    inicio = time.time()

    caminho_entrada = info_etapa4['caminho_saida']

    # Dicionário para acumular agregações
    agg_cnpj = {}

    print(f"\n5.1 - Agregando dados por CNPJ raiz...")

    chunks_processados = 0

    for chunk in pd.read_csv(
        caminho_entrada,
        sep=';',
        encoding='utf-8-sig',
        dtype=str,
        chunksize=CHUNK_SIZE,
        low_memory=False
    ):
        chunks_processados += 1

        # Converter colunas numéricas
        chunk['VALOR_CONSOLIDADO_NUM'] = pd.to_numeric(chunk['VALOR_CONSOLIDADO_NUM'], errors='coerce').fillna(0)
        chunk['FLAG_GARANTIDA'] = pd.to_numeric(chunk['FLAG_GARANTIDA'], errors='coerce').fillna(0).astype(int)
        chunk['FLAG_AJUIZADO'] = pd.to_numeric(chunk['FLAG_AJUIZADO'], errors='coerce').fillna(0).astype(int)
        chunk['FLAG_MATRIZ'] = pd.to_numeric(chunk['FLAG_MATRIZ'], errors='coerce').fillna(0).astype(int)
        chunk['ANO_INSCRICAO'] = pd.to_numeric(chunk['ANO_INSCRICAO'], errors='coerce')
        chunk['DATA_INSCRICAO_DT'] = pd.to_datetime(chunk['DATA_INSCRICAO_DT'], errors='coerce')

        for cnpj_raiz, grupo in chunk.groupby('CNPJ_RAIZ'):
            if cnpj_raiz not in agg_cnpj:
                agg_cnpj[cnpj_raiz] = {
                    'TOTAL_INSCRICOES': 0,
                    'FILIAIS': set(),
                    'UFS': set(),
                    'RECEITAS': set(),
                    'UNIDADES': set(),
                    'GRUPOS_TRIB': set(),
                    'TOTAL_AJUIZADO': 0,
                    'TOTAL_GARANTIDAS': 0,
                    'TOTAL_SG': 0,
                    'TOTAL_PENHORA': 0,
                    'TOTAL_DEPOSITO': 0,
                    'TOTAL_FIANCA': 0,
                    'VALOR_TOTAL': 0.0,
                    'VALOR_GARANTIDO': 0.0,
                    'VALOR_NAO_GARANTIDO': 0.0,
                    'VALORES': [],  # Para mediana
                    'DATA_MIN': None,
                    'DATA_MAX': None,
                    'NOME': None,
                    'QTD_12M': 0,
                    'QTD_24M': 0,
                }

            agg = agg_cnpj[cnpj_raiz]

            agg['TOTAL_INSCRICOES'] += len(grupo)
            agg['FILIAIS'].update(grupo['CPF_CNPJ'].dropna().unique())

            if 'UF_DEVEDOR' in grupo.columns:
                agg['UFS'].update(grupo['UF_DEVEDOR'].dropna().unique())
            if 'RECEITA_PRINCIPAL' in grupo.columns:
                agg['RECEITAS'].update(grupo['RECEITA_PRINCIPAL'].dropna().unique())
            if 'UNIDADE_RESPONSAVEL' in grupo.columns:
                agg['UNIDADES'].update(grupo['UNIDADE_RESPONSAVEL'].dropna().unique())
            if 'GRUPO_TRIBUTARIO' in grupo.columns:
                agg['GRUPOS_TRIB'].update(grupo['GRUPO_TRIBUTARIO'].dropna().unique())

            agg['TOTAL_AJUIZADO'] += grupo['FLAG_AJUIZADO'].sum()
            agg['TOTAL_GARANTIDAS'] += grupo['FLAG_GARANTIDA'].sum()

            # Tipos de garantia
            if 'TIPO_GARANTIA' in grupo.columns:
                agg['TOTAL_SG'] += (grupo['TIPO_GARANTIA'] == 'SEGURO_GARANTIA').sum()
                agg['TOTAL_PENHORA'] += (grupo['TIPO_GARANTIA'] == 'PENHORA').sum()
                agg['TOTAL_DEPOSITO'] += (grupo['TIPO_GARANTIA'].isin(['DEPOSITO_JUDICIAL', 'DEPOSITO_EXECUCAO'])).sum()
                agg['TOTAL_FIANCA'] += (grupo['TIPO_GARANTIA'] == 'CARTA_FIANCA').sum()

            # Valores
            agg['VALOR_TOTAL'] += grupo['VALOR_CONSOLIDADO_NUM'].sum()
            agg['VALOR_GARANTIDO'] += grupo.loc[grupo['FLAG_GARANTIDA'] == 1, 'VALOR_CONSOLIDADO_NUM'].sum()
            agg['VALOR_NAO_GARANTIDO'] += grupo.loc[grupo['FLAG_GARANTIDA'] == 0, 'VALOR_CONSOLIDADO_NUM'].sum()

            # Para mediana (limitar para não estourar memória)
            if len(agg['VALORES']) < 1000:
                agg['VALORES'].extend(grupo['VALOR_CONSOLIDADO_NUM'].head(50).tolist())

            # Datas
            datas = grupo['DATA_INSCRICAO_DT'].dropna()
            if len(datas) > 0:
                data_min = datas.min()
                data_max = datas.max()
                if agg['DATA_MIN'] is None or data_min < agg['DATA_MIN']:
                    agg['DATA_MIN'] = data_min
                if agg['DATA_MAX'] is None or data_max > agg['DATA_MAX']:
                    agg['DATA_MAX'] = data_max

            # Nome
            if agg['NOME'] is None and 'NOME_DEVEDOR' in grupo.columns:
                # Preferir matriz
                matrizes = grupo[grupo['FLAG_MATRIZ'] == 1]
                if len(matrizes) > 0:
                    agg['NOME'] = matrizes['NOME_DEVEDOR'].iloc[0]
                else:
                    agg['NOME'] = grupo['NOME_DEVEDOR'].iloc[0]

            # Inscrições recentes (12M e 24M)
            anos = grupo['ANO_INSCRICAO'].dropna()
            agg['QTD_12M'] += (anos >= 2025).sum()
            agg['QTD_24M'] += (anos >= 2024).sum()

        if chunks_processados % 5 == 0:
            print(f"    Chunk {chunks_processados}: {len(agg_cnpj):,} CNPJs raiz agregados")
            gc.collect()

    # Criar DataFrame final
    print(f"\n5.2 - Gerando DataFrame agregado...")

    rows = []
    for cnpj_raiz, agg in agg_cnpj.items():
        # Calcular métricas derivadas
        valor_mediano = np.median(agg['VALORES']) if agg['VALORES'] else 0
        valor_medio = agg['VALOR_TOTAL'] / agg['TOTAL_INSCRICOES'] if agg['TOTAL_INSCRICOES'] > 0 else 0
        valor_max = max(agg['VALORES']) if agg['VALORES'] else 0

        taxa_ajuiz = agg['TOTAL_AJUIZADO'] / agg['TOTAL_INSCRICOES'] if agg['TOTAL_INSCRICOES'] > 0 else 0
        taxa_garantia = agg['TOTAL_GARANTIDAS'] / agg['TOTAL_INSCRICOES'] if agg['TOTAL_INSCRICOES'] > 0 else 0
        taxa_garantia_ajuiz = agg['TOTAL_GARANTIDAS'] / agg['TOTAL_AJUIZADO'] if agg['TOTAL_AJUIZADO'] > 0 else 0

        span_meses = 0
        if agg['DATA_MIN'] and agg['DATA_MAX']:
            span_meses = (agg['DATA_MAX'] - agg['DATA_MIN']).days / 30.44

        # Proxy de porte
        porte = calcular_proxy_porte(agg['GRUPOS_TRIB'], agg['TOTAL_INSCRICOES'], agg['VALOR_TOTAL'])

        # Flags
        flag_misto = 1 if (agg['TOTAL_GARANTIDAS'] > 0 and agg['TOTAL_GARANTIDAS'] < agg['TOTAL_INSCRICOES']) else 0
        flag_multi_filial = 1 if len(agg['FILIAIS']) > 1 else 0
        flag_multi_uf = 1 if len(agg['UFS']) > 1 else 0

        # Delta 12M
        qtt_12_24m = agg['QTD_24M'] - agg['QTD_12M']
        delta_12m = agg['QTD_12M'] - qtt_12_24m

        # Grupo tributário predominante
        grupo_pred = max(agg['GRUPOS_TRIB'], key=lambda x: x) if agg['GRUPOS_TRIB'] else 'OUTROS'

        # UF principal (mode)
        uf_principal = list(agg['UFS'])[0] if agg['UFS'] else ''

        rows.append({
            'CNPJ_RAIZ': cnpj_raiz,
            'NOME_DEVEDOR': agg['NOME'],
            'UF_PRINCIPAL': uf_principal,
            'TOTAL_INSCRICOES_CNPJ': agg['TOTAL_INSCRICOES'],
            'TOTAL_FILIAIS_CNPJ': len(agg['FILIAIS']),
            'TOTAL_AJUIZADO_CNPJ': agg['TOTAL_AJUIZADO'],
            'TOTAL_GARANTIDAS_CNPJ': agg['TOTAL_GARANTIDAS'],
            'TOTAL_SG_CNPJ': agg['TOTAL_SG'],
            'TOTAL_PENHORA_CNPJ': agg['TOTAL_PENHORA'],
            'TOTAL_DEPOSITO_CNPJ': agg['TOTAL_DEPOSITO'],
            'TOTAL_FIANCA_CNPJ': agg['TOTAL_FIANCA'],
            'VALOR_TOTAL_CNPJ': agg['VALOR_TOTAL'],
            'VALOR_MEDIO_CNPJ': valor_medio,
            'VALOR_MEDIANO_CNPJ': valor_mediano,
            'VALOR_MAX_CNPJ': valor_max,
            'VALOR_TOTAL_GARANTIDO_CNPJ': agg['VALOR_GARANTIDO'],
            'VALOR_TOTAL_NAO_GARANTIDO_CNPJ': agg['VALOR_NAO_GARANTIDO'],
            'TAXA_AJUIZAMENTO_CNPJ': taxa_ajuiz,
            'TAXA_GARANTIA_CNPJ': taxa_garantia,
            'TAXA_GARANTIA_AJUIZADO_CNPJ': taxa_garantia_ajuiz,
            'INSCRICAO_MAIS_ANTIGA': agg['DATA_MIN'],
            'INSCRICAO_MAIS_RECENTE': agg['DATA_MAX'],
            'SPAN_TEMPORAL_CNPJ_MESES': round(span_meses, 0),
            'QTD_INSCRICOES_12M': agg['QTD_12M'],
            'QTD_INSCRICOES_24M': agg['QTD_24M'],
            'DELTA_INSCRICOES_12M': delta_12m,
            'QTD_UF_CNPJ': len(agg['UFS']),
            'QTD_RECEITAS_CNPJ': len(agg['RECEITAS']),
            'QTD_UNIDADES_RESP_CNPJ': len(agg['UNIDADES']),
            'GRUPO_TRIB_PREDOMINANTE_CNPJ': grupo_pred,
            'PORTE_PROXY': porte,
            'FLAG_MISTO_GARANTIA': flag_misto,
            'FLAG_MULTI_FILIAL': flag_multi_filial,
            'FLAG_MULTI_UF': flag_multi_uf,
            'FLAG_CRESCIMENTO_RECENTE': 1 if delta_12m > 0 else 0,
        })

    df_agg = pd.DataFrame(rows)
    df_agg = df_agg.sort_values('VALOR_TOTAL_CNPJ', ascending=False)

    # Adicionar colunas formatadas
    df_agg['VALOR_TOTAL_CNPJ_FMT'] = df_agg['VALOR_TOTAL_CNPJ'].apply(fmt_brl)
    df_agg['VALOR_MEDIO_CNPJ_FMT'] = df_agg['VALOR_MEDIO_CNPJ'].apply(fmt_brl)
    df_agg['VALOR_MEDIANO_CNPJ_FMT'] = df_agg['VALOR_MEDIANO_CNPJ'].apply(fmt_brl)
    df_agg['TAXA_AJUIZAMENTO_CNPJ_FMT'] = df_agg['TAXA_AJUIZAMENTO_CNPJ'].apply(fmt_pct)
    df_agg['TAXA_GARANTIA_CNPJ_FMT'] = df_agg['TAXA_GARANTIA_CNPJ'].apply(fmt_pct)

    # Salvar
    caminho_saida = output_dir / "pgfn_agregado_cnpj_raiz_perfil.csv"
    df_agg.to_csv(caminho_saida, sep=';', index=False, encoding='utf-8-sig')

    tempo = time.time() - inicio

    print(f"\n[OK] Agregado por CNPJ raiz salvo: {caminho_saida}")
    print(f"\n5.3 - Estatísticas:")
    print(f"    Total CNPJs raiz: {fmt_num(len(df_agg))}")
    print(f"    CNPJs com FLAG_MISTO_GARANTIA=1: {fmt_num((df_agg['FLAG_MISTO_GARANTIA'] == 1).sum())}")
    print(f"\n    Distribuição PORTE_PROXY:")
    for porte in ['ALTO', 'MEDIO', 'BAIXO']:
        qtd = (df_agg['PORTE_PROXY'] == porte).sum()
        print(f"      {porte}: {fmt_num(qtd)} ({fmt_pct(qtd/len(df_agg))})")
    print(f"\n    Tempo de execução: {tempo:.1f}s")

    return {
        'df_agg': df_agg,
        'total_cnpj_raiz': len(df_agg),
        'tempo': tempo
    }

# ============================================================================
# ETAPA 6: RELATÓRIO DE SEGMENTAÇÃO
# ============================================================================

def etapa6_relatorio(output_dir, info_etapa1, info_etapa2, info_etapa3, info_etapa4, info_etapa5):
    """
    Etapa 6: Gera relatório completo de segmentação.
    """
    print("\n" + "=" * 80)
    print("ETAPA 6 - RELATÓRIO DE SEGMENTAÇÃO")
    print("=" * 80)

    relatorio = []
    r = relatorio.append

    r("=" * 80)
    r("PIPELINE A — SEGMENTAÇÃO DO UNIVERSO ANALISÁVEL")
    r(f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    r(f"Input: pgfn_unificada_cnpj_principal_semibruta.csv ({fmt_num(info_etapa1['total_linhas'])} linhas)")
    r("NOTA: Todos os valores em formato brasileiro (R$ 1.234.567,89 | 12,34%)")
    r("=" * 80)

    r("\n" + "-" * 60)
    r("1. MAPEAMENTO DE COLUNAS")
    r("-" * 60)
    r(f"   Encoding: {info_etapa1['encoding']}")
    r(f"   Coluna CNPJ: {info_etapa1['col_cnpj']}")

    r("\n" + "-" * 60)
    r("2. CNPJ RAIZ")
    r("-" * 60)
    r(f"   Total CNPJs completos: {fmt_num(info_etapa1['total_cnpjs'])}")
    r(f"   Total CNPJs raiz: {fmt_num(info_etapa1['total_raiz'])}")
    r(f"   Empresas multi-filial: {fmt_num(info_etapa1['multi_filial'])} ({fmt_pct(info_etapa1['multi_filial']/info_etapa1['total_raiz'])})")
    r(f"   Max filiais por CNPJ raiz: {info_etapa1['max_filiais']}")

    r("\n" + "-" * 60)
    r("3. EXCLUSÕES (cascata — sem sobreposição)")
    r("-" * 60)

    r("\n   3.1 Simples Nacional / MEI (exclusão total por CNPJ raiz)")
    r(f"       CNPJs raiz excluídos: {fmt_num(len(info_etapa2['cnpjs_raiz_simples']))}")
    r(f"       Inscrições excluídas: {fmt_num(info_etapa2['total_excluidas'])} ({fmt_pct(info_etapa2['total_excluidas']/info_etapa1['total_linhas'])})")
    r(f"       Valor total excluído: {fmt_brl(info_etapa2['valor_excluido'])}")
    r("       Nota: inclui empresas com MIX de tributos que contenham qualquer")
    r("             inscrição Simples/MEI")

    r("\n   3.2 Recuperação Judicial / Falência / Massa Falida")
    r(f"       CNPJs raiz identificados: {fmt_num(len(info_etapa3['cnpjs_raiz_rj']))}")
    r(f"       Inscrições: {fmt_num(info_etapa3['total_rj'])} ({fmt_pct(info_etapa3['total_rj']/info_etapa1['total_linhas'])})")
    r(f"       Valor total: {fmt_brl(info_etapa3['valor_rj'])}")
    r("       Nota: verificar log_rj_falencia_termos_detectados.csv")

    r("\n   3.3 Mega-corporações segregadas")
    r(f"       CNPJs raiz identificados: {fmt_num(len(info_etapa3['cnpjs_raiz_megacorp']))}")
    r(f"       Inscrições: {fmt_num(info_etapa3['total_mega'])} ({fmt_pct(info_etapa3['total_mega']/info_etapa1['total_linhas'])})")
    r(f"       Valor total: {fmt_brl(info_etapa3['valor_mega'])}")
    r("       Nota: verificar log_mega_corps_identificadas.csv")

    r("\n" + "-" * 60)
    r("4. UNIVERSO ANALISÁVEL RESULTANTE")
    r("-" * 60)
    r(f"   Inscrições: {fmt_num(info_etapa4['total_linhas'])} ({fmt_pct(info_etapa4['total_linhas']/info_etapa1['total_linhas'])})")
    r(f"   CNPJs raiz: {fmt_num(info_etapa5['total_cnpj_raiz'])}")

    # Distribuição por porte
    df_agg = info_etapa5['df_agg']
    r("\n   Distribuição por proxy de porte:")
    for porte in ['ALTO', 'MEDIO', 'BAIXO']:
        qtd = (df_agg['PORTE_PROXY'] == porte).sum()
        valor = df_agg.loc[df_agg['PORTE_PROXY'] == porte, 'VALOR_TOTAL_CNPJ'].sum()
        r(f"     - {porte}: {fmt_num(qtd)} CNPJs raiz ({fmt_brl(valor)})")

    # CNPJs mistos
    mistos = (df_agg['FLAG_MISTO_GARANTIA'] == 1).sum()
    r(f"\n   CNPJs com perfil misto (FLAG_MISTO_GARANTIA=1): {fmt_num(mistos)}")

    r("\n   Top 10 CNPJs raiz por valor total:")
    r(f"   {'CNPJ_RAIZ':<12} | {'NOME':<30} | {'VALOR_TOTAL':>18} | {'INSC':>6} | {'PORTE':>6}")
    r("   " + "-" * 85)
    for _, row in df_agg.head(10).iterrows():
        nome = str(row['NOME_DEVEDOR'])[:30] if row['NOME_DEVEDOR'] else 'N/A'
        r(f"   {row['CNPJ_RAIZ']:<12} | {nome:<30} | {fmt_brl(row['VALOR_TOTAL_CNPJ']):>18} | {row['TOTAL_INSCRICOES_CNPJ']:>6} | {row['PORTE_PROXY']:>6}")

    r("\n" + "-" * 60)
    r("5. ALERTAS DE QUALIDADE")
    r("-" * 60)
    r(f"   - CNPJs com <14 dígitos descartados: {fmt_num(info_etapa1['cnpj_invalidos'])}")
    r(f"   - Datas inválidas: {fmt_num(info_etapa4['erros_data'])}")
    r(f"   - Valores não convertidos: {fmt_num(info_etapa4['erros_valor'])}")

    r("\n" + "-" * 60)
    r("6. TEMPO DE EXECUÇÃO")
    r("-" * 60)
    tempo_total = info_etapa1['tempo'] + info_etapa2['tempo'] + info_etapa3['tempo'] + info_etapa4['tempo'] + info_etapa5['tempo']
    r(f"   Etapa 1 (Mapeamento): {info_etapa1['tempo']:.1f}s")
    r(f"   Etapa 2 (Simples/MEI): {info_etapa2['tempo']:.1f}s")
    r(f"   Etapa 3 (RJ/Mega-corps): {info_etapa3['tempo']:.1f}s")
    r(f"   Etapa 4 (Base limpa): {info_etapa4['tempo']:.1f}s")
    r(f"   Etapa 5 (Agregação): {info_etapa5['tempo']:.1f}s")
    r(f"   Total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")

    r("\n" + "-" * 60)
    r("7. VALIDAÇÃO")
    r("-" * 60)
    total_outputs = info_etapa2['total_excluidas'] + info_etapa3['total_rj'] + info_etapa3['total_mega'] + info_etapa4['total_linhas']
    r(f"   Total nos 4 outputs: {fmt_num(total_outputs)}")
    r(f"   Total original: {fmt_num(info_etapa1['total_linhas'])}")
    r(f"   Diferença: {fmt_num(total_outputs - info_etapa1['total_linhas'])}")
    if total_outputs == info_etapa1['total_linhas']:
        r("   [OK] Validação passou - soma dos outputs = total original")
    else:
        r("   [ALERTA] Validação falhou - verificar sobreposição")

    r("\n" + "=" * 80)
    r("FIM DO RELATÓRIO")
    r("=" * 80)

    # Salvar relatório
    caminho_relatorio = output_dir / "relatorio_segmentacao.txt"
    with open(caminho_relatorio, "w", encoding="utf-8") as f:
        f.write("\n".join(relatorio))

    print(f"\n[OK] Relatório salvo: {caminho_relatorio}")
    print("\n" + "\n".join(relatorio))

# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Pipeline A - Segmentação do Universo Analisável')
    parser.add_argument('--etapa', type=str, default='todas',
                        help='Etapa a executar: 1, 2, 3, 4, 5, 6 ou "todas"')
    parser.add_argument('--teste', action='store_true',
                        help='Modo teste: processa apenas 100.000 linhas')
    args = parser.parse_args()

    print("=" * 80)
    print("PIPELINE A — SEGMENTAÇÃO DO UNIVERSO ANALISÁVEL")
    print("=" * 80)
    print(f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"Diretório de dados: {DATA_DIR}")
    print(f"Modo teste: {'SIM' if args.teste else 'NÃO'}")
    print(f"Etapa(s): {args.etapa}")

    caminho_arquivo = DATA_DIR / ARQUIVO_ENTRADA

    if not caminho_arquivo.exists():
        print(f"\n[ERRO] Arquivo não encontrado: {caminho_arquivo}")
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Diretório de saída: {OUTPUT_DIR}")

    # Executar etapas
    if args.etapa in ['1', 'todas']:
        info_etapa1 = etapa1_mapeamento(caminho_arquivo, OUTPUT_DIR, args.teste)
        if info_etapa1 is None:
            return
    else:
        # Carregar info da etapa 1 de execução anterior
        info_etapa1 = {'encoding': 'utf-8-sig', 'col_cnpj': 'CPF_CNPJ'}

    if args.etapa in ['2', 'todas']:
        info_etapa2 = etapa2_exclusao_simples_mei(caminho_arquivo, OUTPUT_DIR, info_etapa1, args.teste)

    if args.etapa in ['3', 'todas']:
        if 'info_etapa2' not in locals():
            print("[ERRO] Execute a etapa 2 primeiro")
            return
        info_etapa3 = etapa3_segregacao_rj_megacorps(OUTPUT_DIR, info_etapa2, args.teste)

    if args.etapa in ['4', 'todas']:
        if 'info_etapa3' not in locals():
            print("[ERRO] Execute a etapa 3 primeiro")
            return
        info_etapa4 = etapa4_base_limpa(OUTPUT_DIR, info_etapa3, args.teste)

    if args.etapa in ['5', 'todas']:
        if 'info_etapa4' not in locals():
            print("[ERRO] Execute a etapa 4 primeiro")
            return
        info_etapa5 = etapa5_agregacao_cnpj_raiz(OUTPUT_DIR, info_etapa4, args.teste)

    if args.etapa in ['6', 'todas']:
        if 'info_etapa5' not in locals():
            print("[ERRO] Execute a etapa 5 primeiro")
            return
        etapa6_relatorio(OUTPUT_DIR, info_etapa1, info_etapa2, info_etapa3, info_etapa4, info_etapa5)

    print("\n" + "=" * 80)
    print("PIPELINE A CONCLUÍDO")
    print("=" * 80)
    print(f"\nArquivos gerados em: {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
