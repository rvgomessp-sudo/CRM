// ============================================================
// CRM V3 — Tipos TypeScript
// Espelho fiel do schema Supabase
// ============================================================

// ---- Funil enxuto: 5 etapas de prospecção ----
// Back-office (minuta, faturamento, emissão) NÃO é coluna de funil.
export type PipelineStage =
  | 'oportunidade'
  | 'abordagem'
  | 'proposta'
  | 'seguradora'
  | 'fechado'

// Taxonomias antigas — mantidas só para LEITURA enquanto a base migra.
export type LegacyStage =
  | 'base_pgfn' | 'enriquecimento' | 'interesse_manifesto' | 'analise_rapida'
  | 'analise_preliminar' | 'proposta_enviada' | 'proposta_comercial'
  | 'aprovado_ad_pagto' | 'submetido_sancor' | 'analise_estruturacao'
  | 'submetido_seguradora' | 'aprovacao_minuta' | 'faturamento' | 'aprovado'
  | 'consultoria' | 'emissao' | 'receita_realizada' | 'reprovado'

export type AnyStage = PipelineStage | LegacyStage

export type TipoFechamento  = 'CONSULTORIA' | 'EMISSAO' | 'AMBOS'

// ---- Desfecho do "Fechado" ----
// GANHO = negócio fechado. ENCERRADO tem duas origens distintas:
// PERDA (decisão do cliente) vs. DISPENSA (decisão da V&F).
// Regra: recusa de seguradora NÃO encerra — a V&F trabalha a reversão.
export type Desfecho = 'GANHO' | 'PERDA' | 'DISPENSA'

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
  // Desfecho (preenchido quando estagio = 'fechado')
  desfecho?:                Desfecho | null
  tipo_fechamento?:         TipoFechamento | null
  motivo_encerramento?:     string | null
  motivo_obs?:              string | null
  fechado_em?:              string | null
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
  em_oportunidade:       number
  em_abordagem:          number
  em_proposta:           number
  em_seguradora:         number
  em_fechado:            number
  convertidos:           number   // fechado + desfecho GANHO
  perdidos:              number   // fechado + desfecho PERDA
  dispensados:           number   // fechado + desfecho DISPENSA
  fechado_sem_desfecho:  number
  total_divida_carteira: number
  divida_convertida:     number
  divida_em_jogo:        number
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
  ganhos:         number
  perdas:         number
  dispensas:      number
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
// Inclui os 5 canônicos + os legados, para que qualquer valor vindo do banco
// tenha rótulo legível durante e depois da migração.
export const STAGE_LABELS: Record<string, string> = {
  // canônicos
  oportunidade:         'Oportunidade',
  abordagem:            'Abordagem',
  proposta:             'Proposta',
  seguradora:           'Seguradora',
  fechado:              'Fechado',
  // legados (somente leitura)
  base_pgfn:            'Base PGFN',
  enriquecimento:       'Enriquecimento',
  interesse_manifesto:  'Interesse Manifesto',
  analise_rapida:       'Análise Rápida',
  analise_preliminar:   'Análise Preliminar',
  proposta_enviada:     'Proposta Enviada',
  proposta_comercial:   'Proposta Comercial',
  aprovado_ad_pagto:    'Aprovado / Ad. Pagto',
  submetido_sancor:     'Submetido à Sancor',
  analise_estruturacao: 'Análise Estruturação',
  submetido_seguradora: 'Submetido à Seguradora',
  aprovacao_minuta:     'Aprovação / Minuta',
  faturamento:          'Faturamento',
  aprovado:             'Aprovado',
  consultoria:          'Consultoria',
  emissao:              'Emissão',
  receita_realizada:    'Receita Realizada',
  reprovado:            'Reprovado',
}

export const STAGE_COLORS: Record<string, string> = {
  oportunidade: 'bg-text-faint/20 text-text-muted',
  abordagem:    'bg-warning/20 text-warning',
  proposta:     'bg-vf-red/20 text-vf-red-light',
  seguradora:   'bg-info/20 text-info',
  fechado:      'bg-success/20 text-success',
}

// ---- Mapa de conversão: qualquer estágio (novo ou legado) → canônico ----
// Regra do negócio: faturamento não é etapa de funil (é evento financeiro);
// minuta/estruturação seguem "com a seguradora"; consultoria/emissão = Ganho.
export const STAGE_TO_CANON: Record<AnyStage, PipelineStage> = {
  // canônicos
  oportunidade: 'oportunidade',
  abordagem:    'abordagem',
  proposta:     'proposta',
  seguradora:   'seguradora',
  fechado:      'fechado',
  // legados
  base_pgfn:            'oportunidade',
  enriquecimento:       'oportunidade',
  interesse_manifesto:  'abordagem',
  analise_rapida:       'abordagem',
  analise_preliminar:   'abordagem',
  proposta_enviada:     'proposta',
  proposta_comercial:   'proposta',
  aprovado_ad_pagto:    'proposta',
  submetido_sancor:     'seguradora',
  aprovado:             'seguradora',
  analise_estruturacao: 'seguradora',
  submetido_seguradora: 'seguradora',
  aprovacao_minuta:     'seguradora',
  faturamento:          'seguradora',
  consultoria:          'fechado',
  emissao:              'fechado',
  receita_realizada:    'fechado',
  reprovado:            'fechado',
}

/** Normaliza qualquer valor de estágio vindo do banco para um dos 5 canônicos. */
export function normalizeStage(s: string | null | undefined): PipelineStage {
  if (!s) return 'oportunidade'
  return STAGE_TO_CANON[s as AnyStage] ?? 'oportunidade'
}

/** Todos os valores de banco que pertencem a cada etapa canônica.
 *  Permite filtrar com .in() e funcionar ANTES e DEPOIS da migração. */
export const STAGE_DB_VALUES: Record<PipelineStage, string[]> =
  (Object.keys(STAGE_TO_CANON) as AnyStage[]).reduce((acc, k) => {
    acc[STAGE_TO_CANON[k]].push(k)
    return acc
  }, { oportunidade: [], abordagem: [], proposta: [], seguradora: [], fechado: [] } as Record<PipelineStage, string[]>)

// ---- Desfecho: labels e motivos ----
export const DESFECHO_LABELS: Record<Desfecho, string> = {
  GANHO:    'Ganho',
  PERDA:    'Perda — decisão do cliente',
  DISPENSA: 'Dispensa — decisão V&F',
}

export const DESFECHO_COLORS: Record<Desfecho, string> = {
  GANHO:    'bg-success/20 text-success',
  PERDA:    'bg-danger/20 text-danger',
  DISPENSA: 'bg-warning/20 text-warning',
}

export const MOTIVOS_PERDA = [
  'Cliente não disposto a pagar',
  'Não assinou termo de compromisso',
  'Outros',
] as const

export const MOTIVOS_DISPENSA = [
  'Risco de reputação',
  'Demanda abaixo do nosso core',
  'Risco de inadimplência',
  'Outros',
] as const

export const TIPO_FECHAMENTO_LABELS: Record<TipoFechamento, string> = {
  CONSULTORIA: 'Consultoria',
  EMISSAO:     'Emissão',
  AMBOS:       'Ambos',
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
  'oportunidade', 'abordagem', 'proposta', 'seguradora', 'fechado',
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
