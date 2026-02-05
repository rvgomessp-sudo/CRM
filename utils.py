"""
Funcoes utilitarias compartilhadas entre os scripts de analise PGFN.

Centraliza: formatacao BRL, extracao de garantia, classificacao de safra,
leitura e filtragem padrao de bases CSV.
"""

import pandas as pd
import numpy as np
from pathlib import Path

import config


# =============================================================================
# FORMATACAO
# =============================================================================

def fmt_brl(valor):
    """Formata valor numerico no padrao brasileiro (R$)."""
    if pd.isna(valor):
        return "R$ -"
    if abs(valor) >= 1e12:
        return f"R$ {valor/1e12:.1f} tri"
    if abs(valor) >= 1e9:
        return f"R$ {valor/1e9:.1f} bi"
    if abs(valor) >= 1e6:
        return f"R$ {valor/1e6:.1f} mi"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_brl_compacto(valor):
    """Formato compacto para tabelas com pouco espaco."""
    if pd.isna(valor):
        return "R$ -"
    if abs(valor) >= 1e12:
        return f"R$ {valor/1e12:.1f} tri"
    if abs(valor) >= 1e9:
        return f"R$ {valor/1e9:.1f} bi"
    if abs(valor) >= 1e6:
        return f"R$ {valor/1e6:.1f} mi"
    if abs(valor) >= 1e3:
        return f"R$ {valor/1e3:.0f} mil"
    return f"R$ {valor:,.0f}"


# =============================================================================
# CLASSIFICACAO / EXTRACAO
# =============================================================================

def extrair_garantia(situacao):
    """Extrai o tipo de garantia a partir da descricao SITUACAO_INSCRICAO."""
    s = str(situacao).upper()
    if "SEGURO GARANTIA" in s:
        return "SEGURO GARANTIA"
    if "PENHORA" in s:
        return "PENHORA"
    if "CARTA FIANCA" in s or "CARTA FIANCA" in s:
        return "CARTA FIANCA"
    if "DEPOSITO" in s or "DEPOSITO" in s:
        return "DEPOSITO"
    return "SEM GARANTIA"


def classificar_safra(idade_anos):
    """Classifica a inscricao em safra (cohort) baseada na idade em anos."""
    if pd.isna(idade_anos):
        return "SEM DATA"
    if idade_anos <= 1:
        return "2024-2025"
    if idade_anos <= 2:
        return "2023"
    if idade_anos <= 3:
        return "2022"
    if idade_anos <= 4:
        return "2021"
    if idade_anos <= 5:
        return "2020"
    if idade_anos <= 6:
        return "2019"
    return "<=2018"


def classificar_cnpj(row):
    """Classifica CNPJ conforme regra de ticket minimo.

    Espera colunas: INSCR_ACIMA_1MI, INSCR_ABAIXO_1MI
    Retorna: EXCLUIR, MANTER_INTEGRAL ou MANTER_FILTRAR
    """
    if row["INSCR_ACIMA_1MI"] == 0:
        return "EXCLUIR"
    if row["INSCR_ABAIXO_1MI"] == 0:
        return "MANTER_INTEGRAL"
    return "MANTER_FILTRAR"


def classificar_estrategico(row):
    """Classificacao estrategica por prioridade comercial.

    Espera colunas: VALOR_TOTAL, TEM_AJUIZAMENTO, QTD_INSCRICOES
    """
    if row["VALOR_TOTAL"] >= 15_000_000 and not row["TEM_AJUIZAMENTO"]:
        return "ALTA PRIORIDADE - SEGURO GARANTIA"
    if row["VALOR_TOTAL"] >= 15_000_000 and row["TEM_AJUIZAMENTO"]:
        return "SUBSTITUICAO / ESTRATEGIA JUDICIAL"
    if row["VALOR_TOTAL"] >= 1_000_000 and row["QTD_INSCRICOES"] >= 5:
        return "ESTRATEGICA - CONSOLIDACAO"
    return "CAUDA / FUNIL"


# =============================================================================
# LEITURA E PROCESSAMENTO DE DADOS
# =============================================================================

def ler_base_pgfn(arquivo, colunas=None, nrows=None):
    """Le um arquivo CSV da PGFN com encoding e separador corretos.

    Args:
        arquivo: Nome do arquivo ou path completo.
        colunas: Lista de colunas a ler (None = todas).
        nrows: Limitar numero de linhas (None = todas).

    Returns:
        DataFrame com os dados lidos.
    """
    filepath = Path(arquivo)
    if not filepath.is_absolute():
        filepath = config.get_data_path(arquivo)

    kwargs = {
        "sep": ";",
        "encoding": "latin1",
        "low_memory": False,
    }
    if colunas:
        kwargs["usecols"] = colunas
    if nrows:
        kwargs["nrows"] = nrows

    if not filepath.exists():
        raise FileNotFoundError(f"Arquivo nao encontrado: {filepath}")

    df = pd.read_csv(filepath, **kwargs)
    return df


def converter_valor_brl(series):
    """Converte coluna de valor no formato brasileiro (1.234,56) para float."""
    return pd.to_numeric(
        series.astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False),
        errors="coerce"
    )


def filtrar_pj(df):
    """Filtra apenas registros de Pessoa Juridica."""
    mask = df["TIPO_PESSOA"].astype(str).str.contains("jur", case=False, na=False)
    return df[mask].copy()


def filtrar_tributos(df, tributos_alvo=None, excluir_termos=None):
    """Filtra tributos estruturais e exclui termos indesejados."""
    tributos = tributos_alvo or config.TRIBUTOS_ALVO
    excluir = excluir_termos or config.EXCLUIR_TERMOS

    mask_alvo = df["RECEITA_PRINCIPAL"].astype(str).str.contains(
        "|".join(tributos), case=False, na=False
    )
    mask_excluir = df["RECEITA_PRINCIPAL"].astype(str).str.contains(
        "|".join(excluir), case=False, na=False
    )
    return df[mask_alvo & ~mask_excluir].copy()


def adicionar_features(df):
    """Adiciona colunas derivadas: GARANTIA_ATUAL, AJUIZADO_BIN, SAFRA."""
    df = df.copy()

    # Garantia
    df["GARANTIA_ATUAL"] = df["SITUACAO_INSCRICAO"].apply(extrair_garantia)

    # Ajuizado (binario)
    df["AJUIZADO_BIN"] = (
        df["INDICADOR_AJUIZADO"].astype(str).str.upper()
        .isin(["SIM", "S", "1", "TRUE"])
    )

    # Data e safra
    if "DATA_INSCRICAO" in df.columns:
        df["DATA_INSCRICAO"] = pd.to_datetime(
            df["DATA_INSCRICAO"], errors="coerce", dayfirst=True
        )
        hoje = pd.Timestamp.today().normalize()
        df["IDADE_ANOS"] = (hoje - df["DATA_INSCRICAO"]).dt.days / 365.25
        df["SAFRA"] = df["IDADE_ANOS"].apply(classificar_safra)

    return df


def pipeline_base(arquivo, colunas=None):
    """Pipeline padrao: ler + filtrar PJ + filtrar tributos + converter valor + features.

    Args:
        arquivo: Nome do arquivo CSV.
        colunas: Colunas a ler (None = COLUNAS_COMPLETAS).

    Returns:
        DataFrame processado.
    """
    cols = colunas or config.COLUNAS_COMPLETAS
    print(f"  Lendo {arquivo}...")
    df = ler_base_pgfn(arquivo, colunas=cols)
    total_bruto = len(df)
    print(f"    Total bruto: {total_bruto:,}")

    df = filtrar_pj(df)
    print(f"    Apos filtro PJ: {len(df):,}")

    df = filtrar_tributos(df)
    print(f"    Apos filtro tributos: {len(df):,}")

    df["VALOR_CONSOLIDADO"] = converter_valor_brl(df["VALOR_CONSOLIDADO"])

    df = adicionar_features(df)

    return df


def salvar_csv(df, filename, sep=";", encoding="utf-8-sig", index=False):
    """Salva DataFrame como CSV no diretorio de saida."""
    filepath = config.get_output_path(filename)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(filepath, sep=sep, encoding=encoding, index=index)
    print(f"  CSV salvo: {filepath}")
    return filepath


def salvar_grafico(fig, filename, dpi=150):
    """Salva figura matplotlib no diretorio de saida."""
    filepath = config.get_output_path(filename)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(filepath, dpi=dpi, bbox_inches="tight")
    print(f"  Grafico salvo: {filepath}")
    return filepath


# =============================================================================
# AGREGACOES
# =============================================================================

def agregar_por_cnpj(df, ticket_minimo=None):
    """Agrega dados por CNPJ com metricas e classificacao de ticket.

    Args:
        df: DataFrame com dados de inscricoes.
        ticket_minimo: Valor minimo por inscricao (default: config.TICKET_MINIMO).

    Returns:
        DataFrame agregado por CNPJ.
    """
    ticket = ticket_minimo or config.TICKET_MINIMO

    cnpj_agg = df.groupby("CPF_CNPJ").agg(
        NOME=("NOME_DEVEDOR", "first"),
        UF=("UF_DEVEDOR", "first"),
        TOTAL_INSCRICOES=("NUMERO_INSCRICAO", "count"),
        DIVIDA_TOTAL=("VALOR_CONSOLIDADO", "sum"),
        INSCR_ACIMA_1MI=("VALOR_CONSOLIDADO", lambda x: (x >= ticket).sum()),
        DIVIDA_ACIMA_1MI=("VALOR_CONSOLIDADO", lambda x: x[x >= ticket].sum()),
        INSCR_ABAIXO_1MI=("VALOR_CONSOLIDADO", lambda x: (x < ticket).sum()),
        DIVIDA_ABAIXO_1MI=("VALOR_CONSOLIDADO", lambda x: x[x < ticket].sum()),
    ).reset_index()

    cnpj_agg["CLASSIFICACAO"] = cnpj_agg.apply(classificar_cnpj, axis=1)

    return cnpj_agg


def calcular_potencial_comercial(divida_enderecavel):
    """Calcula premio e comissao potenciais.

    Returns:
        Tuple (premio, comissao)
    """
    premio = divida_enderecavel * config.TAXA_PREMIO
    comissao = premio * config.TAXA_COMISSAO
    return premio, comissao
