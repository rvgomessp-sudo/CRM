'use client'

// ============================================================
// Modal de desfecho — captura o "sim/não e suas características"
// ao mover uma oportunidade para Fechado.
//   GANHO    → tipo de fechamento (Consultoria / Emissão / Ambos)
//   PERDA    → decisão do CLIENTE  (motivos + livre)
//   DISPENSA → decisão da V&F      (motivos + livre)
// Regra: recusa de seguradora NÃO chega aqui — não encerra oportunidade.
// ============================================================
import { useState } from 'react'
import { cn } from '@/lib/utils'
import {
  DESFECHO_LABELS, MOTIVOS_PERDA, MOTIVOS_DISPENSA, TIPO_FECHAMENTO_LABELS,
  type Desfecho, type TipoFechamento,
} from '@/lib/types'
import { Trophy, XCircle, ShieldOff, X } from 'lucide-react'

export interface DesfechoPayload {
  desfecho:             Desfecho
  tipo_fechamento:      TipoFechamento | null
  motivo_encerramento:  string | null
  motivo_obs:           string | null
}

const OPCOES: { key: Desfecho; icon: typeof Trophy; desc: string; on: string; icon_on: string }[] = [
  { key: 'GANHO',    icon: Trophy,    desc: 'Negócio fechado',    on: 'border-success bg-success/10', icon_on: 'text-success' },
  { key: 'PERDA',    icon: XCircle,   desc: 'Decisão do cliente', on: 'border-danger bg-danger/10',   icon_on: 'text-danger'  },
  { key: 'DISPENSA', icon: ShieldOff, desc: 'Decisão da V&F',     on: 'border-warning bg-warning/10', icon_on: 'text-warning' },
]

export function DesfechoModal({ empresaNome, initial, onConfirm, onCancel, saving }: {
  empresaNome: string
  initial?: Partial<DesfechoPayload>
  onConfirm: (p: DesfechoPayload) => void
  onCancel: () => void
  saving?: boolean
}) {
  const [desfecho, setDesfecho] = useState<Desfecho | null>(initial?.desfecho ?? null)
  const [tipo, setTipo]         = useState<TipoFechamento | null>(initial?.tipo_fechamento ?? null)
  const [motivo, setMotivo]     = useState<string | null>(initial?.motivo_encerramento ?? null)
  const [obs, setObs]           = useState(initial?.motivo_obs ?? '')

  const motivos   = desfecho === 'PERDA' ? MOTIVOS_PERDA : desfecho === 'DISPENSA' ? MOTIVOS_DISPENSA : []
  const precisaObs = motivo === 'Outros'

  const podeConfirmar = desfecho === 'GANHO'
    ? !!tipo
    : desfecho != null && !!motivo && (!precisaObs || obs.trim().length > 0)

  function confirmar() {
    if (!desfecho || !podeConfirmar) return
    onConfirm({
      desfecho,
      tipo_fechamento:     desfecho === 'GANHO' ? tipo : null,
      motivo_encerramento: desfecho === 'GANHO' ? null : motivo,
      motivo_obs:          obs.trim() || null,
    })
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4" onClick={onCancel}>
      <div
        className="bg-bg-secondary border border-border rounded-xl w-full max-w-md p-5 shadow-2xl"
        onClick={e => e.stopPropagation()}
      >
        <div className="flex items-start justify-between mb-1">
          <h3 className="text-text-primary font-semibold text-sm">Fechar oportunidade</h3>
          <button onClick={onCancel} className="text-text-faint hover:text-text-primary"><X className="w-4 h-4" /></button>
        </div>
        <p className="text-text-faint text-xs mb-4 truncate">{empresaNome}</p>

        {/* Desfecho */}
        <div className="grid grid-cols-3 gap-2 mb-4">
          {OPCOES.map(({ key, icon: Icon, desc, on, icon_on }) => {
            const ativo = desfecho === key
            return (
              <button key={key}
                onClick={() => { setDesfecho(key); setMotivo(null); setTipo(null) }}
                className={cn('flex flex-col items-center gap-1 rounded-lg border p-2.5 text-center transition-colors',
                  ativo ? on : 'border-border hover:border-text-faint')}
              >
                <Icon className={cn('w-4 h-4', ativo ? icon_on : 'text-text-muted')} />
                <span className={cn('text-[11px] font-medium', ativo ? 'text-text-primary' : 'text-text-muted')}>
                  {DESFECHO_LABELS[key].split(' — ')[0]}
                </span>
                <span className="text-[9px] text-text-faint leading-tight">{desc}</span>
              </button>
            )
          })}
        </div>

        {/* GANHO → tipo de fechamento */}
        {desfecho === 'GANHO' && (
          <div className="mb-4">
            <label className="text-text-faint text-[11px] block mb-1.5">Tipo de fechamento</label>
            <div className="flex gap-2">
              {(Object.keys(TIPO_FECHAMENTO_LABELS) as TipoFechamento[]).map(t => (
                <button key={t} onClick={() => setTipo(t)}
                  className={cn('flex-1 rounded-lg border py-1.5 text-[11px] transition-colors',
                    tipo === t ? 'border-vf-red bg-vf-red/10 text-vf-red-light'
                               : 'border-border text-text-muted hover:border-text-faint')}>
                  {TIPO_FECHAMENTO_LABELS[t]}
                </button>
              ))}
            </div>
            <p className="text-text-faint text-[10px] mt-1.5 leading-snug">
              Honorários faturam antes e/ou depois da apólice; comissão só após a apólice emitida.
            </p>
          </div>
        )}

        {/* PERDA / DISPENSA → motivo */}
        {(desfecho === 'PERDA' || desfecho === 'DISPENSA') && (
          <div className="mb-4">
            <label className="text-text-faint text-[11px] block mb-1.5">Motivo</label>
            <div className="space-y-1.5">
              {motivos.map(m => (
                <button key={m} onClick={() => setMotivo(m)}
                  className={cn('w-full text-left rounded-lg border px-3 py-1.5 text-[11px] transition-colors',
                    motivo === m ? 'border-vf-red bg-vf-red/10 text-text-primary'
                                 : 'border-border text-text-muted hover:border-text-faint')}>
                  {m}
                </button>
              ))}
            </div>
            {precisaObs && (
              <textarea autoFocus value={obs} onChange={e => setObs(e.target.value)}
                placeholder="Descreva o motivo…"
                className="input text-sm w-full mt-2 h-16 resize-none" />
            )}
          </div>
        )}

        {/* Ações */}
        <div className="flex justify-end gap-2 mt-1">
          <button onClick={onCancel} className="btn-ghost text-xs">Cancelar</button>
          <button onClick={confirmar} disabled={!podeConfirmar || saving}
            className="btn-primary text-xs disabled:opacity-40">
            {saving ? 'Salvando…' : 'Confirmar fechamento'}
          </button>
        </div>
      </div>
    </div>
  )
}
