# CRM PGFN - Analise de Dividas Judiciais

Sistema de analise de dados da PGFN (Procuradoria-Geral da Fazenda Nacional) para identificacao de oportunidades no mercado de Seguro Garantia Judicial.

## Objetivo

Processar e analisar bases de dados de dividas ativas da PGFN para identificar empresas (PJ) com dividas ajuizadas sem garantia, qualificando-as como prospects para Seguro Garantia Judicial.

## Estrutura do Projeto

```
CRM/
├── config.py                              # Configuracao centralizada (paths, constantes)
├── utils.py                               # Funcoes utilitarias compartilhadas
├── requirements.txt                       # Dependencias Python
├── README.md                              # Este arquivo
│
├── claude_analise_basePGFN_1_2025.py      # Analise exploratoria inicial
├── arquivo_lai_SIDA_1_202512_impostos.py  # Analise por tributo e empresa
├── analise_pgfn_pj_tributos.py            # Analise PJ + tributos estruturais
├── analise_garantias_ajuizados.py         # Analise de garantias em ajuizados
├── analise_cruzada_situacao.py            # Cruzamento TIPO_SITUACAO x SITUACAO
├── analise_2020_2025_base_limpa.py        # Base limpa com corte temporal 2020-2025
├── analise_hipoteses_safra_ticket.py      # Testes de hipoteses (Chi2, ticket)
├── consolidar_6_bases_pgfn.py             # Consolidacao das 6 bases regionais
└── consolidar_classificar_pgfn_final.py   # Pipeline final de classificacao
```

## Configuracao

### 1. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 2. Configurar diretorio de dados

Os scripts esperam encontrar os CSVs da PGFN em um diretorio. Ha duas formas de configurar:

**Opcao A: Variavel de ambiente**
```bash
export PGFN_DATA_DIR="/caminho/para/os/csvs"
```

**Opcao B: Editar config.py**
Altere a variavel `DATA_DIR` em `config.py`:
```python
DATA_DIR = Path("/caminho/para/os/csvs")
```

### 3. Arquivos de dados esperados

Os scripts processam 6 bases regionais:
- `arquivo_lai_SIDA_1_202512.csv` (1a Regiao - DF/GO/MT/TO/Norte)
- `arquivo_lai_SIDA_2_202512.csv` (2a Regiao - RJ/ES)
- `arquivo_lai_SIDA_3_202512.csv` (3a Regiao - SP/MS)
- `arquivo_lai_SIDA_4_202512.csv` (4a Regiao - RS/SC/PR)
- `arquivo_lai_SIDA_5_202512.csv` (5a Regiao - PE/AL/PB/RN/CE/SE)
- `arquivo_lai_SIDA_6_202512.csv` (6a Regiao - MG)

Formato: CSV com separador `;`, encoding Latin-1.

## Scripts de Analise

### Ordem recomendada de execucao

1. **`claude_analise_basePGFN_1_2025.py`** - Exploracao inicial (visao geral)
2. **`arquivo_lai_SIDA_1_202512_impostos.py`** - Analise detalhada por tributo
3. **`analise_pgfn_pj_tributos.py`** - Analise PJ com cruzamentos
4. **`analise_garantias_ajuizados.py`** - Foco em tipos de garantia
5. **`analise_cruzada_situacao.py`** - Validacao de filtros
6. **`consolidar_6_bases_pgfn.py`** - Consolidacao nacional
7. **`analise_2020_2025_base_limpa.py`** - Base limpa com corte temporal
8. **`analise_hipoteses_safra_ticket.py`** - Testes estatisticos
9. **`consolidar_classificar_pgfn_final.py`** - Classificacao final

### Executar um script

```bash
python consolidar_6_bases_pgfn.py
```

## Filtros Aplicados

| Filtro | Criterio |
|--------|----------|
| Tipo de pessoa | Pessoa Juridica |
| Tributos alvo | COFINS, PIS, IRPJ, CSLL, IPI, IOF, Imp. Importacao |
| Exclusoes | SIMPLES, MEI, IRRF, MULTA, CUSTAS |
| Ajuizamento | INDICADOR_AJUIZADO = SIM |
| Garantia | SEM GARANTIA |
| Corte temporal | Safras 2020-2025 |
| Ticket minimo | R$ 1.000.000 por inscricao |

## Parametros Comerciais

- **Taxa de premio**: 2% do valor da divida
- **Taxa de comissao**: 25% do premio (0,5% da divida)

Configuraveis em `config.py`.

## Saidas Geradas

### CSVs
- `consolidado_6_bases_pgfn.csv` - Resumo das 6 bases
- `cnpjs_2020_2025_classificados.csv` - CNPJs com classificacao de ticket
- `cnpjs_2020_2025_base_final.csv` - Base final de prospects
- `inscricoes_2020_2025_base_final.csv` - Inscricoes qualificadas
- `cnpj_consolidado_classificado_final.csv` - Classificacao estrategica

### Graficos (PNG)
- `consolidado_6_bases_pgfn.png` - Painel consolidado nacional
- `analise_2020_2025_base_limpa.png` - Dashboard 2020-2025
- `analise_hipoteses_safra_ticket.png` - Graficos de hipoteses
- `analise_exploratoria_sida1.png` - Visao geral exploratoria

## Arquitetura

```
config.py         -> Configuracao centralizada
utils.py          -> Funcoes reutilizaveis (leitura, filtros, formatacao)
scripts/*.py      -> Scripts de analise (usam config + utils)
```

Todos os scripts importam `config` e `utils`, eliminando duplicacao de codigo
e paths hardcoded.

## VF CRM (Interface Web)

O arquivo `VF_CRM.html` e o CRM completo da Vazquez & Fonseca, executavel direto no navegador (sem servidor). Funcionalidades:

- **Pipeline Kanban** com 8 estagios (Identificado → Pos-venda)
- **Ficha de Empresa** com dados PGFN, enriquecimento financeiro e elegibilidade por seguradora
- **Seguradoras & Regras** (Sancor, Berkley, Zurich/Swiss/Chubb)
- **Gerador de Proposta** com calculo automatico de premio, honorarios e comissao
- **Biblioteca de Documentos** com controle de sigilo
- **Google Sheets** como backend (via Apps Script)
- **Import XLSX/CSV/JSON** de dados do pipeline Python

### Integracao Pipeline Python → CRM

```bash
# 1. Executar pipeline de segmentacao
python segmentacao_universo.py

# 2. Exportar para formato CRM
python exportar_para_crm.py

# 3. Abrir VF_CRM.html no navegador e importar o .xlsx ou .json gerado
```

O script `exportar_para_crm.py` le o CSV agregado por CNPJ raiz e gera:
- **XLSX** com abas Sancor/Berkley/Zurich (para import via botao no CRM)
- **JSON** no formato de backup do CRM (importavel direto)
- **CSV** com todas as empresas qualificadas
