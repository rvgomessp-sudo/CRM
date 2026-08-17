// ============================================================
// CRM V3 — Tipos TypeScript
// Espelho fiel do schema Supabase
// ============================================================

export type PipelineStage =
  | 'base_pgfn'
  | 'enriquecimento'
  | 'abordagem'
  | 'analise_preliminar'
  | 'proposta_comercial'
  | 'aprovado_ad_pagto'
  | 'analise_estruturacao'
  | 'submetido_seguradora'
  | 'aprovacao_minuta'
  | 'faturamento'
  | 'consultoria'
  | 'emissao'
  | 'reprovado'

export type TipoFechamento  = 'CONSULTORIA' | 'EMISSAO' | 'AMBOS'

export type PrioridadeTipo  = 'ALTA' | 'MEDIA' | 'BAIXA'
export type MotorTipo       = 'A1' | 'A2' | 'B1' | 'B2' | 'B3' | 'B4' | 'B5'
export type SeguradoraTipo  = 'SANCOR' | 'BERKLEY' | 'ZURICH' | 'SWISS' | 'CHUBB'
export type FaixaTipo       = 'F1_SANCOR' | 'F2_AMPLIADA'
export type PapelUsuario    = 'admin' | 'operador'
export type CanalInteracao  = 'EMAIL' | 'TELEFONE' | 'WHATSAPP' | 'REUNIAO' | 'SISTEMA'
export type StatusConsulta  = 'PENDENTE' | 'APROVADO' | 'RECUSADO' | 'CONDICIONAL'
export type StatusProposta  = 'RASCUNHO' | 'ENVIADA' | 'EM_ANALISE' | 'APROVADA' | 'RECUSADA' | 'CONVERTIDA'

// ---- Profile ----
export interface Profile {
  id:           string
  nome:         string
  email:        string
  papel:        PapelUsuario
  ativo:        boolean
  criado_em:    string
  atualizado_em: string
}

// ---- Empresa ----
export interface Empresa {
  cnpj_raiz:                string   // 8 dígitos
  cnpj_completo:            string
  nome_devedor:             string
  uf_devedor:               string | null
  qtd_inscricoes:           number
  prioridade:               PrioridadeTipo | null
  motor:                    MotorTipo | null
  faixa:                    FaixaTipo | null
  valor_total_devida:       number
  valor_maior_inscricao:    number
  estagio:                  PipelineStage
  seguradora_alvo:          SeguradoraTipo
  responsavel_id:           string | null
  ativo:                    boolean
  excluido:                 boolean
  motivo_exclusao:          string | null
  // Enriquecimento
  capital_social:           number | null
  receita_estimada:         number | null
  pl_estimado:              number | null
  regime_tributario:        string | null
  cnpj_situacao:            string | null
  cnae_principal:           string | null
  segmento:                 string | null
  // Decisor
  decisor_nome:             string | null
  decisor_cargo:            string | null
  decisor_email:            string | null
  decisor_telefone:         string | null
  decisor_linkedin:         string | null
  // Score
  score_vf:                 number | null
  notas:                    string | null
  // SLA
  ultimo_contato_em:        string | null
  proxima_acao_em:          string | null
  proxima_acao_descricao:   string | null
  // Proteção
  nda_assinado:             boolean
  nda_data:                 string | null
  // Inteligência (P0–F2)
  socios?:                  { nome?: string; qualificacao?: string }[] | null
  tributo_principal?:       string | null
  zona_risco?:              string | null
  evento_judicial_tipo?:    string | null
  evento_judicial_em?:      string | null
  eventos_trabalhistas?:    number | null
  evento_trabalhista_em?:   string | null
  // Metadados
  importado_em:             string
  criado_em:                string
  atualizado_em:            string
  atualizado_por:           string | null
  // Joins opcionais
  responsavel?:             Profile | null
  inscricoes?:              Inscricao[]
  interacoes?:              Interacao[]
}

// ---- Inscrição ----
export interface Inscricao {
  id:                   string
  cnpj_raiz:            string
  cnpj_completo:        string
  nome_devedor:         string | null
  uf_devedor:           string | null
  numero_inscricao:     string
  situacao_inscricao:   string | null
  tipo_garantia:        string | null
  flag_garantia:        string | null
  tributo:              string | null
  receita_principal:    string | null
  data_inscricao:       string | null
  dias_inscricao:       number | null
  ano_inscricao:        number | null
  valor_brl:            string | null
  valor_numerico:       number | null
  indicador_ajuizado:   boolean
  unidade_responsavel:  string | null
  motor:                MotorTipo | null
  prioridade:           PrioridadeTipo | null
  criado_em:            string
}

// ---- Interação ----
export interface Interacao {
  id:                   string
  cnpj_raiz:            string
  canal:                CanalInteracao
  resumo:               string
  proxima_acao:         string | null
  proxima_acao_em:      string | null
  responsavel_id:       string | null
  estagio_na_interacao: PipelineStage | null
  criado_em:            string
  criado_por:           string | null
  // Joins
  responsavel?:         Profile | null
}

// ---- Consulta Seguradora ----
export interface ConsultaSeguradora {
  id:               string
  cnpj_raiz:        string
  seguradora:       SeguradoraTipo
  status:           StatusConsulta
  limite_aprovado:  number | null
  taxa_indicativa:  number | null
  modalidade:       string | null
  notas:            string | null
  data_consulta:    string
  validade_ate:     string | null
  criado_em:        string
  criado_por:       string | null
}

// ---- Proposta ----
export interface Proposta {
  id:                   string
  cnpj_raiz:            string
  valor_garantia:       number
  inscricoes_cobertas:  string[] | null
  seguradora:           SeguradoraTipo
  taxa_anual:           number
  prazo_anos:           number
  premio_bruto:         number
  comissao_pct:         number | null
  comissao_valor:       number | null
  honorarios_valor:     number
  receita_vf_total:     number | null
  regra_economica_ok:   boolean | null
  status:               StatusProposta
  data_envio:           string | null
  validade_proposta:    string | null
  notas:                string | null
  pdf_url:              string | null
  criado_em:            string
  criado_por:           string | null
  atualizado_em:        string
}

// ---- Histórico de Estágio ----
export interface HistoricoEstagio {
  id:               string
  cnpj_raiz:        string
  estagio_anterior: PipelineStage | null
  estagio_novo:     PipelineStage
  mudado_por:       string | null
  mudado_em:        string
  observacao:       string | null
}

// ---- Views ----
export interface DashboardKPIs {
  total_empresas:        number
  em_base:               number
  em_enriquecimento:     number
  em_abordagem:          number
  em_interesse:          number
  em_analise:            number
  em_proposta:           number
  convertidos:           number
  total_divida_carteira: number
  divida_convertida:     number
  motor_a1:              number
  motor_a2:              number
  motor_b1:              number
  motor_b2:              number
  followups_vencidos:    number
}

export interface FunilEstagio {
  estagio:        PipelineStage
  qtd_empresas:   number
  valor_total:    number
  score_medio:    number | null
}

// ---- CSV Import ----
export interface CSVRow {
  CNPJ_RAIZ:            string
  CNPJ_COMPLETO:        string
  NOME_DEVEDOR:         string
  UF_DEVEDOR:           string
  QTD_INSCRICOES_EMPRESA: string
  PRIORIDADE_MARINHEIRO: string
  NUMERO_INSCRICAO:     string
  SITUACAO_INSCRICAO:   string
  TIPO_GARANTIA:        string
  FLAG_GARANTIA:        string
  TRIBUTO:              string
  RECEITA_PRINCIPAL:    string
  DATA_INSCRICAO:       string
  DIAS_INSCRICAO:       string
  ANO_INSCRICAO:        string
  VALOR_BRL:            string
  VALOR_NUMERICO:       string
  INDICADOR_AJUIZADO:   string
  UNIDADE_RESPONSAVEL:  string
}

// ---- VF Solver ----
export interface SolverInput {
  valorGarantia:  number
  taxaAnual:      number    // decimal: 0.005 = 0,50%
  prazoAnos:      number
  comissaoPct:    number    // decimal: 0.20 = 20%
  honorarios:     number
  seguradora:     SeguradoraTipo
}

export interface SolverOutput {
  premioBruto:      number
  comissaoValor:    number
  receiaVFTotal:    number
  regraEconomica:   boolean
  taxaEfetiva:      number
  alertas:          string[]
}

// ---- Labels ----
export const STAGE_LABELS: Record<PipelineStage, string> = {
  base_pgfn:            'Base PGFN',
  enriquecimento:       'Enriquecimento',
  abordagem:            'Abordagem',
  analise_preliminar:   'Análise Preliminar',
  proposta_comercial:   'Proposta Comercial',
  aprovado_ad_pagto:    'Aprovado / Ad. Pagto',
  analise_estruturacao: 'Análise Estruturação',
  submetido_seguradora: 'Submetido à Seguradora',
  aprovacao_minuta:     'Aprovação / Minuta',
  faturamento:          'Faturamento',
  consultoria:          'Consultoria',
  emissao:              'Emissão',
  reprovado:            'Reprovado',
}

export const STAGE_COLORS: Record<PipelineStage, string> = {
  base_pgfn:            'bg-text-faint text-text-muted',
  enriquecimento:       'bg-info/20 text-info',
  abordagem:            'bg-warning/20 text-warning',
  analise_preliminar:   'bg-amber-500/20 text-amber-400',
  proposta_comercial:   'bg-purple-500/20 text-purple-400',
  aprovado_ad_pagto:    'bg-cyan-500/20 text-cyan-400',
  analise_estruturacao: 'bg-indigo-500/20 text-indigo-400',
  submetido_seguradora: 'bg-vf-red/20 text-vf-red-light',
  aprovacao_minuta:     'bg-vf-red/30 text-vf-red-light',
  faturamento:          'bg-success/30 text-success',
  consultoria:          'bg-emerald-500/30 text-emerald-400',
  emissao:              'bg-green-900/50 text-green-400',
  reprovado:            'bg-red-900/40 text-red-400',
}

export const MOTOR_LABELS: Record<MotorTipo, string> = {
  A1: 'A1 – Urgência',
  A2: 'A2 – Prevenção',
  B1: 'B1 – Penhora',
  B2: 'B2 – Seguro Garantia',
  B3: 'B3 – Carta Fiança',
  B4: 'B4 – Depósito',
  B5: 'B5 – NJP',
}

// Alias para compatibilidade
export const MOTOR_BADGE_LABEL = MOTOR_LABELS

export const MOTOR_COLORS: Record<MotorTipo, string> = {
  A1: 'bg-red-500/20 text-red-400',
  A2: 'bg-amber-500/20 text-amber-400',
  B1: 'bg-blue-500/20 text-blue-400',
  B2: 'bg-purple-500/20 text-purple-400',
  B3: 'bg-teal-500/20 text-teal-400',
  B4: 'bg-cyan-500/20 text-cyan-400',
  B5: 'bg-pink-500/20 text-pink-400',
}

export const STAGES_ORDERED: PipelineStage[] = [
  'base_pgfn', 'enriquecimento', 'abordagem', 'analise_preliminar',
  'proposta_comercial', 'aprovado_ad_pagto', 'analise_estruturacao',
  'submetido_seguradora', 'aprovacao_minuta', 'faturamento',
  'consultoria', 'emissao', 'reprovado',
]

// ============================================================
// P0/P1 — Eventos & Oportunidades (fila priorizada)
// ============================================================

export type FonteEvento   = 'FISCAL' | 'LICITACAO' | 'JUDICIAL'
export type TriagemStatus = 'NOVO' | 'VISTO' | 'DESCARTADO' | 'ABORDAR'

// Linha da view vw_fila_oportunidades
export interface FilaRow {
  oportunidade_id:        string
  cnpj_raiz:              string
  cnpj_completo:          string
  nome_devedor:           string
  uf_devedor:             string | null
  fonte:                  FonteEvento
  motor:                  MotorTipo | null
  estagio:                PipelineStage
  triagem:                TriagemStatus
  triado_em:              string | null
  motivo_descarte:        string | null
  qtd_inscricoes:         number
  valor_total_devida:     number
  valor_maior_inscricao:  number
  capital_social:         number | null
  cnpj_situacao:          string | null
  tributo_principal:      string | null
  ratio_divida_capital:   number | null
  evento_judicial_tipo:   string | null
  evento_judicial_em:     string | null
  zona_risco:             string | null
  eventos_trabalhistas:   number | null
  evento_trabalhista_em:  string | null
  seguradora_alvo:        SeguradoraTipo
  prioridade:             PrioridadeTipo | null
  score:                  number | null
  alvo_marinheiro:        boolean
}

export const FONTE_LABELS: Record<FonteEvento, string> = {
  FISCAL:    'Fiscal (PGFN)',
  LICITACAO: 'Licitação (PNCP)',
  JUDICIAL:  'Judicial (DJE)',
}

export const TRIAGEM_LABELS: Record<TriagemStatus, string> = {
  NOVO:       'Novo',
  VISTO:      'Visto',
  DESCARTADO: 'Descartado',
  ABORDAR:    'Abordar',
}

export const TRIAGEM_COLORS: Record<TriagemStatus, string> = {
  NOVO:       'bg-info/20 text-info',
  VISTO:      'bg-text-faint/20 text-text-muted',
  DESCARTADO: 'bg-danger/20 text-danger',
  ABORDAR:    'bg-success/20 text-success',
}


export const EVENTO_JUDICIAL_LABELS: Record<string, string> = {
  SISBAJUD:          'SISBAJUD',
  PENHORA:           'Penhora',
  RENAJUD:           'RENAJUD',
  CITACAO:           'Citação',
  EXECUCAO_FISCAL:   'Execução Fiscal',
  ANULATORIA:        'Anulatória',
  EMBARGOS_EXEC:     'Embargos',
  MANDADO_SEGURANCA: 'Mand. Segurança',
  CAUTELAR:          'Cautelar',
}

// movimentos de constrição = alerta máximo (o "sufoco")
export const EVENTO_ALERTA_MAXIMO = new Set(['SISBAJUD','PENHORA','RENAJUD'])


// Zonas de risco processual (mapa de guerra fiscal)
export const ZONA_LABELS: Record<string, string> = {
  SUFOCO:   'Sufoco',
  VERMELHA: 'Vermelha',
  AMARELA:  'Amarela',
}
export const ZONA_COLORS: Record<string, string> = {
  SUFOCO:   'bg-danger/20 text-danger',
  VERMELHA: 'bg-orange-500/20 text-orange-400',
  AMARELA:  'bg-amber-400/15 text-amber-300',
}
export const ZONA_DESCRICAO: Record<string, string> = {
  SUFOCO:   'Constrição FISCAL recente (SISBAJUD/penhora) — caixa travado, urgência máxima.',
  VERMELHA: 'Execução fiscal ajuizada — cronômetro ligado; risco concreto de bloqueio.',
  AMARELA:  'Inscrita em dívida ativa, sem execução localizada — janela de prevenção.',
}
