// lib/types.ts

export type EstagioPipeline =
  | 'base_pgfn'
  | 'enriquecimento'
  | 'abordagem'
  | 'interesse_manifesto'
  | 'analise_rapida'
  | 'proposta_enviada'
  | 'submetido_sancor'
  | 'aprovado'
  | 'fechado'
  | 'receita_realizada'

export type PrioridadeMarinheiro = 'ALTA' | 'MEDIA' | 'BAIXA'
export type SeguradoraTier = 'Sancor' | 'Berkley' | 'Zurich' | 'Swiss Re' | 'Chubb' | 'Indefinida'
export type StatusProposta = 'rascunho' | 'enviada' | 'em_negociacao' | 'aceita' | 'recusada' | 'expirada'
export type CanalInteracao = 'email' | 'telefone' | 'whatsapp' | 'reuniao' | 'sistema' | 'outro'

export interface Empresa {
  cnpj_raiz: string
  cnpj_completo: string | null
  nome_devedor: string
  uf_devedor: string | null
  qtd_inscricoes_empresa: number | null
  prioridade: PrioridadeMarinheiro | null
  seguradora: SeguradoraTier
  estagio: EstagioPipeline
  responsavel_id: string | null
  valor_total_brl: number | null
  valor_minimo_inscricao: number | null
  valor_maximo_inscricao: number | null
  pl_estimado: number | null
  receita_estimada: number | null
  regime_tributario: string | null
  sancor_consultado: boolean
  sancor_aprovado: boolean | null
  sancor_limite: number | null
  sancor_taxa_minima: number | null
  sancor_data_consulta: string | null
  excluido: boolean
  motivo_exclusao: string | null
  fonte: string | null
  importado_em: string
  atualizado_em: string
  // joins opcionais
  inscricoes?: Inscricao[]
  interacoes?: Interacao[]
  decisores?: Decisor[]
  propostas?: Proposta[]
}

export interface Inscricao {
  id: string
  cnpj_raiz: string
  numero_inscricao: string
  situacao_inscricao: string | null
  tipo_garantia: string | null
  flag_garantia: string | null
  tributo: string | null
  receita_principal: string | null
  data_inscricao: string | null
  dias_inscricao: number | null
  ano_inscricao: number | null
  valor_brl: string | null
  valor_numerico: number | null
  indicador_ajuizado: boolean
  unidade_responsavel: string | null
  criado_em: string
}

export interface Interacao {
  id: string
  cnpj_raiz: string
  canal: CanalInteracao
  resumo: string
  proxima_acao: string | null
  data_proxima_acao: string | null
  responsavel_id: string | null
  estagio_momento: EstagioPipeline | null
  criado_em: string
  usuario?: { nome: string }
}

export interface Proposta {
  id: string
  cnpj_raiz: string
  seguradora: SeguradoraTier
  status: StatusProposta
  valor_garantia: number
  taxa_aa: number
  prazo_meses: number
  premio_bruto: number | null
  comissao_pct: number
  comissao_brl: number | null
  honorarios_brl: number | null
  receita_vf_total: number | null
  premio_liquido: number | null
  regra_economica_ok: boolean | null
  enviada_em: string | null
  aceita_em: string | null
  observacoes: string | null
  criado_em: string
  atualizado_em: string
}

export interface Decisor {
  id: string
  cnpj_raiz: string
  nome: string
  cargo: string | null
  email: string | null
  telefone: string | null
  linkedin: string | null
  principal: boolean
  nda_assinado: boolean
  nda_data: string | null
}

export interface Usuario {
  id: string
  auth_user_id: string | null
  nome: string
  email: string
  papel: 'admin' | 'operador'
  ativo: boolean
}

// Labels de exibição
export const ESTAGIO_LABELS: Record<EstagioPipeline, string> = {
  base_pgfn: '1. Base PGFN',
  enriquecimento: '2. Enriquecimento',
  abordagem: '3. Abordagem',
  interesse_manifesto: '4. Interesse Manifesto',
  analise_rapida: '5. Análise Rápida',
  proposta_enviada: '6. Proposta Enviada',
  submetido_sancor: '7. Submetido Sancor',
  aprovado: '8. Aprovado',
  fechado: '9. Fechado',
  receita_realizada: '10. Receita Realizada',
}

export const ESTAGIO_ORDEM: EstagioPipeline[] = [
  'base_pgfn', 'enriquecimento', 'abordagem', 'interesse_manifesto',
  'analise_rapida', 'proposta_enviada', 'submetido_sancor',
  'aprovado', 'fechado', 'receita_realizada'
]

// Campos obrigatórios por estágio (regras de passagem)
export const ESTAGIO_CAMPOS_OBRIGATORIOS: Partial<Record<EstagioPipeline, string[]>> = {
  enriquecimento: ['cnpj_completo', 'uf_devedor'],
  interesse_manifesto: ['responsavel_id'],
  analise_rapida: ['sancor_consultado'],
  proposta_enviada: ['sancor_aprovado'],
  submetido_sancor: ['sancor_limite'],
}
