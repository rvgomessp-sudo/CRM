// ============================================================
// Componentes de inteligência — o "volante" comum a todas as telas.
// Score, zona de risco e evento fiscal renderizados do mesmo jeito
// na fila de oportunidades, na Base PGFN e no Pipeline.
// Sem hooks: servem em Server e Client Components.
// ============================================================
import { cn } from '@/lib/utils'
import {
  EVENTO_JUDICIAL_LABELS, EVENTO_ALERTA_MAXIMO, ZONA_LABELS, ZONA_COLORS,
} from '@/lib/types'
import { Flame, Gavel, AlertTriangle } from 'lucide-react'

function diasDe(d: string | null | undefined): number | null {
  if (!d) return null
  return Math.floor((Date.now() - new Date(d).getTime()) / 86400000)
}

/** Badge de score 0–100 — mesma régua da fila de oportunidades. */
export function ScoreBadge({ score, className }: { score: number | null | undefined; className?: string }) {
  const s = score ?? null
  return (
    <span className={cn('inline-flex items-center justify-center w-9 h-6 rounded font-bold text-xs',
      s == null ? 'bg-bg-hover text-text-faint' :
      s >= 80   ? 'bg-vf-red/25 text-vf-red-light' :
      s >= 55   ? 'bg-amber-500/20 text-amber-400' :
                  'bg-bg-hover text-text-muted',
      className,
    )}>
      {s != null ? Math.round(s) : '—'}
    </span>
  )
}

/** Badge da zona de risco processual (mapa de guerra fiscal). */
export function ZonaBadge({ zona }: { zona: string | null | undefined }) {
  if (!zona) return <span className="text-text-faint text-xs">—</span>
  return (
    <span className={cn('badge text-[10px]', ZONA_COLORS[zona] ?? 'bg-bg-hover text-text-muted')}>
      {zona === 'SUFOCO' && '🚨 '}{ZONA_LABELS[zona] ?? zona}
    </span>
  )
}

/**
 * Evento fiscal mais grave + contador trabalhista.
 * Trabalhista fica separado de propósito: é sinal de estresse de caixa,
 * sem relação securitária tributária.
 */
export function EventoFiscalCell({ tipo, em, trabalhistas }: {
  tipo: string | null | undefined
  em: string | null | undefined
  trabalhistas?: number | null
}) {
  return (
    <>
      {tipo ? (() => {
        const d = diasDe(em)
        const fresco = d != null && d <= 30
        const alerta = EVENTO_ALERTA_MAXIMO.has(tipo)
        const Icon = alerta ? AlertTriangle : Gavel
        return (
          <div className={cn('flex items-center gap-1', alerta && 'px-1.5 py-0.5 rounded bg-danger/15 -ml-1.5')}>
            <Icon className={cn('w-3 h-3 flex-shrink-0',
              alerta ? 'text-danger' : fresco ? 'text-vf-red-light' : 'text-text-faint')} />
            <span className={cn('text-[11px]',
              alerta ? 'text-danger font-semibold' : fresco ? 'text-text-primary' : 'text-text-muted')}>
              {EVENTO_JUDICIAL_LABELS[tipo] ?? tipo}
              {d != null && <span className={alerta ? 'text-danger/70' : 'text-text-faint'}> · {d === 0 ? 'hoje' : `${d}d`}</span>}
            </span>
          </div>
        )
      })() : <span className="text-text-faint text-xs">—</span>}
      {trabalhistas ? (
        <span className="block text-[10px] text-text-faint mt-0.5"
          title="Constrições trabalhistas — sinal de estresse de caixa, sem relação securitária tributária">
          ⚒ {trabalhistas} trabalhista{trabalhistas > 1 ? 's' : ''}
        </span>
      ) : null}
    </>
  )
}

/** Chama trabalho — marca visual do marinheiro (alvo prioritário). */
export function MarinheiroFlame({ on }: { on: boolean | null | undefined }) {
  if (!on) return null
  return <Flame className="w-3 h-3 text-vf-red-light flex-shrink-0" />
}
