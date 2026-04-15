// lib/utils.ts
import { EstagioPipeline, Empresa, ESTAGIO_CAMPOS_OBRIGATORIOS } from './types'

// ── Formatação BRL (sem centavos) ──────────────────────────
export function formatBRL(value: number | null | undefined): string {
  if (value == null) return '—'
  return new Intl.NumberFormat('pt-BR', {
    style: 'currency',
    currency: 'BRL',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value)
}

// ── CNPJ_RAIZ a partir de qualquer formato ────────────────
export function extrairCnpjRaiz(cnpjRaw: string): string {
  const apenasDigitos = cnpjRaw.replace(/[^0-9]/g, '')
  return apenasDigitos.substring(0, 8).padStart(8, '0')
}

// ── Formatar CNPJ completo ─────────────────────────────────
export function formatarCnpj(cnpj: string): string {
  const d = cnpj.replace(/[^0-9]/g, '')
  if (d.length !== 14) return cnpj
  return `${d.slice(0,2)}.${d.slice(2,5)}.${d.slice(5,8)}/${d.slice(8,12)}-${d.slice(12,14)}`
}

// ── Prioridade marinheiro ─────────────────────────────────
export function calcularPrioridade(qtd: number): 'ALTA' | 'MEDIA' | 'BAIXA' {
  if (qtd <= 2) return 'ALTA'
  if (qtd <= 4) return 'MEDIA'
  return 'BAIXA'
}

// ── Seguradora sugerida por valor ─────────────────────────
export function sugerirSeguradora(valorTotal: number): string {
  if (valorTotal <= 20_000_000) return 'Sancor'
  if (valorTotal <= 30_000_000) return 'Berkley'
  return 'Zurich'
}

// ── Validação de passagem de estágio ─────────────────────
export function podeProceder(empresa: Empresa, proximoEstagio: EstagioPipeline): {
  ok: boolean
  camposFaltando: string[]
} {
  const campos = ESTAGIO_CAMPOS_OBRIGATORIOS[proximoEstagio] || []
  const faltando = campos.filter(campo => {
    const val = (empresa as Record<string, unknown>)[campo]
    return val == null || val === false || val === ''
  })
  return { ok: faltando.length === 0, camposFaltando: faltando }
}

// ── Calcular prêmio e receita VF ─────────────────────────
export function calcularProposta({
  valorGarantia,
  taxaAA,
  prazoMeses,
  comissaoPct,
  honorariosBRL,
}: {
  valorGarantia: number
  taxaAA: number
  prazoMeses: number
  comissaoPct: number
  honorariosBRL: number
}) {
  const premioBruto = valorGarantia * taxaAA * (prazoMeses / 12)
  const comissaoBRL = premioBruto * (comissaoPct / 100)
  const premioLiquido = premioBruto - comissaoBRL
  const receitaVF = comissaoBRL + honorariosBRL
  const regraEconomicaOk = receitaVF > premioLiquido

  return {
    premioBruto,
    comissaoBRL,
    premioLiquido,
    receitaVF,
    regraEconomicaOk,
  }
}

// ── Dias úteis entre duas datas ───────────────────────────
export function diasUteis(dataInicio: Date, dataFim: Date): number {
  let count = 0
  const cur = new Date(dataInicio)
  while (cur <= dataFim) {
    const dow = cur.getDay()
    if (dow !== 0 && dow !== 6) count++
    cur.setDate(cur.getDate() + 1)
  }
  return count
}

// ── Parse de valor BRL do CSV (ex: "R$ 4.119.813") ───────
export function parseBRLParaNumerico(valorBRL: string): number {
  const limpo = valorBRL.replace(/[^0-9,]/g, '').replace(',', '.')
  return parseFloat(limpo.replace(/\./g, '').replace(',', '.')) || 0
}

// ── Verificar exclusão automática ────────────────────────
export function deveExcluir(nomeDevedor: string, regimeTributario?: string | null): {
  excluir: boolean
  motivo?: string
} {
  const nome = nomeDevedor.toUpperCase()
  if (nome.includes('MASSA FALIDA')) return { excluir: true, motivo: 'Massa Falida' }
  if (nome.includes('RECUPERAÇÃO JUDICIAL') || nome.includes('RECUPERACAO JUDICIAL')) return { excluir: true, motivo: 'Recuperação Judicial' }
  if (nome.includes('FALIDO')) return { excluir: true, motivo: 'Falido' }
  if (regimeTributario?.toUpperCase().includes('SIMPLES')) return { excluir: true, motivo: 'Simples Nacional' }
  if (regimeTributario?.toUpperCase().includes('MEI')) return { excluir: true, motivo: 'MEI' }
  return { excluir: false }
}

// ── Cor por prioridade ────────────────────────────────────
export function corPrioridade(prioridade: string | null): string {
  switch (prioridade) {
    case 'ALTA': return '#dc2626'
    case 'MEDIA': return '#d97706'
    case 'BAIXA': return '#6b7280'
    default: return '#9ca3af'
  }
}
