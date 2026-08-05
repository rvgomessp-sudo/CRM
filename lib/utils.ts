import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'

// ---- Tailwind classnames utility ----
export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

// ---- CNPJ Raiz ----
// Regra: remove TODOS os não-numéricos → pega os 8 primeiros dígitos
// Preserva zero à esquerda. Resultado sempre CHAR(8).
export function extractCnpjRaiz(rawCnpj: string): string {
  const digits = rawCnpj.replace(/\D/g, '')        // remove tudo que não é dígito
  const raiz = digits.substring(0, 8)              // primeiros 8 dígitos
  return raiz.padStart(8, '0')                      // garante 8 chars (segurança)
}

// Formata CNPJ completo: XX.XXX.XXX/XXXX-XX
export function formatCnpj(cnpj: string): string {
  const d = cnpj.replace(/\D/g, '')
  if (d.length !== 14) return cnpj
  return `${d.slice(0,2)}.${d.slice(2,5)}.${d.slice(5,8)}/${d.slice(8,12)}-${d.slice(12)}`
}

// ---- Formatação monetária BRL ----
const BRL = new Intl.NumberFormat('pt-BR', {
  style: 'currency',
  currency: 'BRL',
  minimumFractionDigits: 0,
  maximumFractionDigits: 0,
})

export function formatBRL(value: number | null | undefined): string {
  if (value == null) return '—'
  return BRL.format(value)
}

export function formatBRLCompact(value: number | null | undefined): string {
  if (value == null) return '—'
  if (Math.abs(value) >= 1_000_000_000) return `R$ ${(value / 1_000_000_000).toFixed(1)}B`
  if (Math.abs(value) >= 1_000_000)     return `R$ ${(value / 1_000_000).toFixed(1)}M`
  if (Math.abs(value) >= 1_000)         return `R$ ${(value / 1_000).toFixed(0)}K`
  return BRL.format(value)
}

// Parseia string BRL do CSV ("R$ 4.119.813" ou "4119812.53")
export function parseBRLString(raw: string): number {
  if (!raw) return 0
  // Remove "R$", espaços, pontos de milhar, substitui vírgula decimal
  const clean = raw
    .replace('R$', '')
    .replace(/\./g, '')
    .replace(',', '.')
    .trim()
  const n = parseFloat(clean)
  return isNaN(n) ? 0 : n
}

// ---- Formatação de taxa ----
export function formatTaxa(taxa: number | null | undefined): string {
  if (taxa == null) return '—'
  return `${(taxa * 100).toFixed(2)}% a.a.`
}

// ---- Formatação de datas ----
export function formatDate(dateStr: string | null | undefined): string {
  if (!dateStr) return '—'
  try {
    const d = new Date(dateStr)
    return d.toLocaleDateString('pt-BR')
  } catch {
    return dateStr
  }
}

export function formatDateTime(dateStr: string | null | undefined): string {
  if (!dateStr) return '—'
  try {
    const d = new Date(dateStr)
    return d.toLocaleString('pt-BR', { dateStyle: 'short', timeStyle: 'short' })
  } catch {
    return dateStr
  }
}

// Dias desde uma data
export function diasDesde(dateStr: string | null | undefined): number {
  if (!dateStr) return 0
  const diff = Date.now() - new Date(dateStr).getTime()
  return Math.floor(diff / (1000 * 60 * 60 * 24))
}

// SLA: retorna classe CSS baseada nos dias parado
export function slaStatus(diasParado: number): 'ok' | 'alerta' | 'vencido' {
  if (diasParado <= 3)  return 'ok'
  if (diasParado <= 7)  return 'alerta'
  return 'vencido'
}

export function slaClass(diasParado: number): string {
  const s = slaStatus(diasParado)
  if (s === 'ok')      return 'text-success'
  if (s === 'alerta')  return 'text-warning'
  return 'text-danger'
}

// ---- Parseia data do CSV (dd/mm/yyyy) ----
export function parseCSVDate(raw: string): string | null {
  if (!raw || raw.trim() === '') return null
  const [d, m, y] = raw.split('/')
  if (!d || !m || !y) return null
  return `${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}`
}

// ---- Prioridade do marinheiro → enum ----
export function mapPrioridade(raw: string): 'ALTA' | 'MEDIA' | 'BAIXA' {
  const p = raw?.toUpperCase().trim()
  if (p === 'ALTA')  return 'ALTA'
  if (p === 'MEDIA') return 'MEDIA'
  return 'BAIXA'
}

// ---- Faixa baseada no valor ----
export function mapFaixa(valorTotal: number): 'F1_SANCOR' | 'F2_AMPLIADA' {
  return valorTotal <= 7_000_000 ? 'F1_SANCOR' : 'F2_AMPLIADA'
}

// ---- Seguradora-alvo baseada no valor ----
export function mapSeguradora(valorTotal: number, pl?: number): 'SANCOR' | 'BERKLEY' | 'ZURICH' {
  if (valorTotal <= 20_000_000) return 'SANCOR'
  if (valorTotal <= 30_000_000) return 'BERKLEY'
  return 'ZURICH'
}

// ---- Abbreviate long names ----
export function truncate(str: string, max: number): string {
  if (str.length <= max) return str
  return str.slice(0, max - 1) + '…'
}

// ---- Pluralizar ----
export function plural(n: number, singular: string, plural: string): string {
  return `${n} ${n === 1 ? singular : plural}`
}
