"""
Configuração centralizada do projeto CRM PGFN.

Todos os paths, constantes e parâmetros configuráveis ficam aqui.
Para ajustar o ambiente, altere apenas DATA_DIR abaixo.
"""

import os
from pathlib import Path

# =============================================================================
# DIRETÓRIO DE DADOS
# =============================================================================
# Altere esta variável para apontar para o diretório onde estão os CSVs PGFN.
# Aceita variável de ambiente PGFN_DATA_DIR como override.
DATA_DIR = Path(os.environ.get(
    "PGFN_DATA_DIR",
    r"C:\Rodrigo\BasePGFN\2025"  # padrão Windows original
))

# Diretório de saída (gráficos, CSVs gerados)
OUTPUT_DIR = Path(os.environ.get(
    "PGFN_OUTPUT_DIR",
    str(DATA_DIR)  # por padrão, salva no mesmo diretório dos dados
))

# =============================================================================
# ARQUIVOS DE ENTRADA (6 bases regionais PGFN)
# =============================================================================
BASES = [
    ("arquivo_lai_SIDA_1_202512.csv", "1a Regiao", "1a Regiao (DF/GO/MT/TO/Norte)"),
    ("arquivo_lai_SIDA_2_202512.csv", "2a Regiao", "2a Regiao (RJ/ES)"),
    ("arquivo_lai_SIDA_3_202512.csv", "3a Regiao", "3a Regiao (SP/MS)"),
    ("arquivo_lai_SIDA_4_202512.csv", "4a Regiao", "4a Regiao (RS/SC/PR)"),
    ("arquivo_lai_SIDA_5_202512.csv", "5a Regiao", "5a Regiao (PE/AL/PB/RN/CE/SE)"),
    ("arquivo_lai_SIDA_6_202512.csv", "6a Regiao", "6a Regiao (MG)"),
]

# =============================================================================
# TRIBUTOS E FILTROS
# =============================================================================
# Tributos estruturais de empresas medio/grande porte
TRIBUTOS_ALVO = [
    "COFINS", "PIS", "IRPJ", "CSLL", "IPI", "IOF", "Imposto de Importacao"
]

# Termos a excluir (ruido / pequeno porte)
EXCLUIR_TERMOS = ["SIMPLES", "MEI", "IRRF", "MULTA", "CUSTAS"]

# =============================================================================
# COLUNAS PADRAO
# =============================================================================
COLUNAS_COMPLETAS = [
    "CPF_CNPJ", "TIPO_PESSOA", "TIPO_DEVEDOR", "NOME_DEVEDOR", "UF_DEVEDOR",
    "UNIDADE_RESPONSAVEL", "NUMERO_INSCRICAO", "TIPO_SITUACAO_INSCRICAO",
    "SITUACAO_INSCRICAO", "RECEITA_PRINCIPAL", "DATA_INSCRICAO",
    "INDICADOR_AJUIZADO", "VALOR_CONSOLIDADO"
]

COLUNAS_REDUZIDAS = [
    "CPF_CNPJ", "TIPO_PESSOA", "NOME_DEVEDOR", "UF_DEVEDOR",
    "NUMERO_INSCRICAO", "SITUACAO_INSCRICAO", "RECEITA_PRINCIPAL",
    "DATA_INSCRICAO", "INDICADOR_AJUIZADO", "VALOR_CONSOLIDADO"
]

# =============================================================================
# PARAMETROS COMERCIAIS
# =============================================================================
TICKET_MINIMO = 1_000_000       # R$ 1 milhao
TAXA_PREMIO = 0.02              # 2% do valor da divida
TAXA_COMISSAO = 0.25            # 25% do premio

# =============================================================================
# CORTE TEMPORAL
# =============================================================================
SAFRAS_VALIDAS = ["2024-2025", "2023", "2022", "2021", "2020"]
IDADE_MAXIMA_ANOS = 6

# =============================================================================
# FAIXAS DE TICKET
# =============================================================================
FAIXAS_TICKET = [
    ("< R$ 500 mil", 0, 500_000),
    ("R$ 500k - R$ 1 mi", 500_000, 1_000_000),
    ("R$ 1 mi - R$ 2 mi", 1_000_000, 2_000_000),
    ("R$ 2 mi - R$ 5 mi", 2_000_000, 5_000_000),
    ("R$ 5 mi - R$ 10 mi", 5_000_000, 10_000_000),
    ("R$ 10 mi - R$ 50 mi", 10_000_000, 50_000_000),
    ("R$ 50 mi - R$ 100 mi", 50_000_000, 100_000_000),
    ("R$ 100 mi - R$ 500 mi", 100_000_000, 500_000_000),
    ("> R$ 500 mi", 500_000_000, float("inf")),
]

# =============================================================================
# VISUALIZACAO
# =============================================================================
CORES_REGIOES = ['#3498db', '#2ecc71', '#e74c3c', '#f39c12', '#9b59b6', '#1abc9c']

CORES_SAFRAS = {
    '2024-2025': '#3498db',
    '2023': '#2ecc71',
    '2022': '#f1c40f',
    '2021': '#e67e22',
    '2020': '#e74c3c',
    '2019': '#9b59b6',
    '<=2018': '#95a5a6',
    'SEM DATA': '#bdc3c7',
}

CORES_CLASSIFICACAO = {
    'MANTER_INTEGRAL': '#27ae60',
    'MANTER_FILTRAR': '#f39c12',
    'EXCLUIR': '#e74c3c',
}

CORES_GARANTIA = {
    'Seguro Garantia': '#27ae60',
    'Carta Fianca': '#3498db',
    'Deposito': '#f39c12',
    'Penhora': '#e74c3c',
    'Sem Garantia': '#95a5a6',
}

# =============================================================================
# HELPERS
# =============================================================================

def get_data_path(filename):
    """Retorna o path completo para um arquivo de dados."""
    return DATA_DIR / filename


def get_output_path(filename):
    """Retorna o path completo para um arquivo de saida."""
    return OUTPUT_DIR / filename
