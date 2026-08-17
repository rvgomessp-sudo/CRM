import type { MotorTipo } from './types'

// ============================================================
// Motor V&F — Classificação de Inscrições PGFN
//
// A1 — Urgência:    Ajuizado + SEM garantia → execução ativa, risco BACENJUD
// A2 — Prevenção:   NÃO ajuizado + SEM garantia → antecipe antes do ajuizamento
// B1 — Otimização:  Situação contém PENHORA → substituição, libere ativos
// B2 — Revisão:     Tipo garantia é SEGURO GARANTIA → revisão de prêmio/renovação
//
// Prioridade: A1 > B1 > B2 > A2
// ============================================================

// Strings de exclusão (inscrições que NÃO entram no pipeline)
const EXCLUSAO_SITUACAO = [
  'PARCELAMENTO',
  'PARCELADA',
  'NEGOCIADO',
  'NEGOCIADA',
  'SUSPENSO',
  'SUSPENSA',
  'SIMPLES',
  'RECUPERA',
  'FALÊNCIA',
  'FALIDO',
  'MASSA FALIDA',
  'CANCELADA',
]

// Verifica se a inscrição deve ser excluída do pipeline
export function deveExcluir(situacao: string): boolean {
  const s = situacao.toUpperCase()
  return EXCLUSAO_SITUACAO.some(exc => s.includes(exc))
}

// Classifica o motor de uma inscrição
export function classificarMotor(
  situacaoInscricao: string,
  tipoGarantia: string,
  indicadorAjuizado: boolean,
): MotorTipo | null {
  const situacao = situacaoInscricao.toUpperCase()
  const garantia = tipoGarantia.toUpperCase()

  // 1. Exclusão direta
  if (deveExcluir(situacao)) return null

  // 2. B1 — Penhora (tem penhora registrada)
  if (situacao.includes('PENHORA')) return 'B1'

  // 3. B2 — Seguro Garantia existente
  if (
    situacao.includes('SEGURO GARANTIA') ||
    garantia.includes('SEGURO') ||
    garantia === 'SEGURO_GARANTIA'
  ) return 'B2'

  // 4. Verificar garantia/parcelamento (exclui A1/A2 se tem garantia)
  if (
    garantia !== 'SEM_GARANTIA' &&
    !garantia.includes('SEM') &&
    garantia !== ''
  ) return null

  // 5. A1 — Ajuizado + sem garantia
  if (indicadorAjuizado) return 'A1'

  // 6. A2 — Não ajuizado + sem garantia
  return 'A2'
}

// Retorna o motor de MAIOR urgência entre uma lista de motores
const MOTOR_PRIORITY: Record<MotorTipo, number> = {
  A1: 1,
  B1: 2,
  B2: 3,
  B3: 4,
  B4: 5,
  B5: 6,
  A2: 7,
}

export function motorMaisUrgente(motores: (MotorTipo | null)[]): MotorTipo | null {
  const validos = motores.filter((m): m is MotorTipo => m !== null)
  if (validos.length === 0) return null
  return validos.sort((a, b) => MOTOR_PRIORITY[a] - MOTOR_PRIORITY[b])[0]
}

// Script de abordagem baseado no motor
export const MOTOR_ABORDAGEM: Record<MotorTipo, string> = {
  A1: 'Execução fiscal ativa – risco de bloqueio BACENJUD. Seguro Garantia suspende a cobrança imediatamente.',
  A2: 'Inscrição ativa pré-judicial. Constitua o Seguro Garantia agora e antecipe o ajuizamento.',
  B1: 'Penhora de ativos em curso. Substituição por Seguro Garantia libera o capital imobilizado.',
  B2: 'Seguro Garantia ativo detectado. Revisão de prêmio e limite pode reduzir custo da apólice.',
  B3: 'Carta de fiança bancária imobilizando limite de crédito. Substituição por Seguro Garantia libera o limite no banco.',
  B4: 'Depósito judicial imobiliza o caixa. Seguro Garantia substitui o depósito e recupera o capital de giro.',
  B5: 'Garantia por patrimônio líquido / NJP. Estruture Seguro Garantia adequado à obrigação e reduza a exposição patrimonial.',
}

export const MOTOR_BADGE_LABEL: Record<MotorTipo, string> = {
  A1: 'A1 URGÊNCIA',
  A2: 'A2 PREVENÇÃO',
  B1: 'B1 PENHORA',
  B2: 'B2 SEGURO GARANTIA',
  B3: 'B3 CARTA FIANÇA',
  B4: 'B4 DEPÓSITO',
  B5: 'B5 NJP',
}
