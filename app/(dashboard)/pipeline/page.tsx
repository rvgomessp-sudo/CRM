'use client'

import { useEffect, useState, useCallback } from 'react'
import Link from 'next/link'
import { createClient } from '@/lib/supabase/client'
import {
  formatBRLCompact, formatDate, diasDesde, slaClass, cn
} from '@/lib/utils'
import {
  STAGES_ORDERED, STAGE_LABELS, MOTOR_COLORS,
  type Empresa, type PipelineStage, type MotorTipo
} from '@/lib/types'
import { AlertTriangle, Filter, RefreshCw, ChevronRight } from 'lucide-react'

type Filters = {
  responsavel: string
  motor: string
  seguradora: string
  prioridade: string
}

export default function PipelinePage() {
  const supabase = createClient()

  const [empresas, setEmpresas] = useState<Empresa[]>([])
  const [profiles, setProfiles] = useState<{id: string; nome: string}[]>([])
  const [loading, setLoading] = useState(true)
  const [filters, setFilters] = useState<Filters>({ responsavel: '', motor: '', seguradora: '', prioridade: '' })
  const [movingId, setMovingId] = useState<string | null>(null)
  const [moveTarget, setMoveTarget] = useState<{cnpj: string; stage: PipelineStage} | null>(null)

  const fetchData = useCallback(async () => {
    setLoading(true)
    let query = supabase
      .from('empresas')
      .select('*, responsavel:profiles(id, nome)')
      .eq('ativo', true)
      .eq('excluido', false)
      .order('atualizado_em', { ascending: false })

    if (filters.responsavel) query = query.eq('responsavel_id', filters.responsavel)
    if (filters.motor) query = query.eq('motor', filters.motor)
    if (filters.seguradora) query = query.eq('seguradora_alvo', filters.seguradora)
    if (filters.prioridade) query = query.eq('prioridade', filters.prioridade)

    const { data } = await query
    setEmpresas(data || [])
    setLoading(false)
  }, [filters])

  useEffect(() => {
    fetchData()
    supabase.from('profiles').select('id, nome').then(({ data }) => setProfiles(data || []))
  }, [fetchData])

  const grouped = STAGES_ORDERED.reduce((acc, stage) => {
    acc[stage] = empresas.filter(e => e.estagio === stage)
    return acc
  }, {} as Record<PipelineStage, Empresa[]>)

  async function moverEstagio(cnpjRaiz: string, novoEstagio: PipelineStage) {
    setMovingId(cnpjRaiz)
    await supabase
      .from('empresas')
      .update({ estagio: novoEstagio, atualizado_em: new Date().toISOString() })
      .eq('cnpj_raiz', cnpjRaiz)
    await fetchData()
    setMovingId(null)
    setMoveTarget(null)
  }

  function updateFilter(key: keyof Filters, value: string) {
    setFilters(prev => ({ ...prev, [key]: value }))
  }

  return (
    <div className="flex flex-col h-screen">

      {/* Toolbar */}
      <div className="px-6 py-4 border-b border-border bg-bg-secondary flex items-center gap-3 flex-wrap">
        <h1 className="text-text-primary font-semibold">Pipeline</h1>
        <span className="text-text-faint text-sm">{empresas.length} empresas</span>

        <div className="flex-1" />

        {/* Filtros */}
        <div className="flex items-center gap-2 text-sm">
          <Filter className="w-3.5 h-3.5 text-text-muted" />

          <select className="select w-auto py-1 text-xs" value={filters.responsavel} onChange={e => updateFilter('responsavel', e.target.value)}>
            <option value="">Responsável</option>
            {profiles.map(p => <option key={p.id} value={p.id}>{p.nome}</option>)}
          </select>

          <select className="select w-auto py-1 text-xs" value={filters.motor} onChange={e => updateFilter('motor', e.target.value)}>
            <option value="">Motor</option>
            {['A1','A2','B1','B2'].map(m => <option key={m} value={m}>{m}</option>)}
          </select>

          <select className="select w-auto py-1 text-xs" value={filters.seguradora} onChange={e => updateFilter('seguradora', e.target.value)}>
            <option value="">Seguradora</option>
            {['SANCOR','BERKLEY','ZURICH','SWISS','CHUBB'].map(s => <option key={s} value={s}>{s}</option>)}
          </select>

          <select className="select w-auto py-1 text-xs" value={filters.prioridade} onChange={e => updateFilter('prioridade', e.target.value)}>
            <option value="">Prioridade</option>
            {['ALTA','MEDIA','BAIXA'].map(p => <option key={p} value={p}>{p}</option>)}
          </select>

          <button onClick={fetchData} className="btn-ghost py-1 text-xs">
            <RefreshCw className="w-3 h-3" /> Atualizar
          </button>
        </div>
      </div>

      {/* Kanban Board */}
      <div className="flex-1 overflow-x-auto p-4">
        <div className="flex gap-3 min-w-max h-full">
          {STAGES_ORDERED.map(stage => {
            const cards = grouped[stage] || []
            const totalValor = cards.reduce((s, e) => s + (e.valor_total_devida || 0), 0)

            return (
              <div key={stage} className="kanban-col">
                {/* Column header */}
                <div className="kanban-col-header">
                  <div>
                    <p className="text-text-primary text-xs font-semibold">{STAGE_LABELS[stage]}</p>
                    {totalValor > 0 && (
                      <p className="text-text-faint text-[10px]">{formatBRLCompact(totalValor)}</p>
                    )}
                  </div>
                  <span className="badge bg-bg-primary text-text-muted text-[10px]">{cards.length}</span>
                </div>

                {/* Cards */}
                <div className="flex-1 overflow-y-auto p-2 space-y-2">
                  {loading ? (
                    <div className="text-text-faint text-xs text-center py-4">Carregando…</div>
                  ) : cards.length === 0 ? (
                    <div className="text-text-faint text-[11px] text-center py-6 border border-dashed border-border rounded-lg">
                      Vazio
                    </div>
                  ) : (
                    cards.map(emp => (
                      <KanbanCard
                        key={emp.cnpj_raiz}
                        empresa={emp}
                        currentStage={stage}
                        moving={movingId === emp.cnpj_raiz}
                        onMove={(novoEstagio) => moverEstagio(emp.cnpj_raiz, novoEstagio)}
                      />
                    ))
                  )}
                </div>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}

// ---- Kanban Card ----
function KanbanCard({
  empresa, currentStage, moving, onMove
}: {
  empresa: Empresa
  currentStage: PipelineStage
  moving: boolean
  onMove: (stage: PipelineStage) => void
}) {
  const [showMove, setShowMove] = useState(false)
  const dias = diasDesde(empresa.atualizado_em)
  const sla = slaClass(dias)

  return (
    <div className={cn('kanban-card', moving && 'opacity-50')}>

      {/* Motor + prioridade */}
      <div className="flex items-center gap-1.5 mb-2">
        {empresa.motor && (
          <span className={cn('badge text-[10px]', MOTOR_COLORS[empresa.motor as MotorTipo])}>
            {empresa.motor}
          </span>
        )}
        {empresa.prioridade === 'ALTA' && (
          <AlertTriangle className="w-3 h-3 text-warning flex-shrink-0" />
        )}
        <span className="text-text-faint text-[10px] ml-auto">{empresa.uf_devedor}</span>
      </div>

      {/* Nome */}
      <Link href={`/empresa/${empresa.cnpj_raiz}`}>
        <p className="text-text-primary text-xs font-medium leading-tight hover:text-vf-red-light line-clamp-2 mb-1">
          {empresa.nome_devedor}
        </p>
      </Link>

      {/* Valor */}
      <p className="text-vf-red-light font-semibold text-sm mb-2">
        {formatBRLCompact(empresa.valor_total_devida)}
      </p>

      {/* Seguradora + SLA */}
      <div className="flex items-center justify-between text-[10px]">
        <span className="text-text-faint">{empresa.seguradora_alvo}</span>
        <span className={cn(sla, 'font-medium')}>{dias}d</span>
      </div>

      {/* Próxima ação */}
      {empresa.proxima_acao_descricao && (
        <p className="text-text-faint text-[10px] mt-1.5 truncate border-t border-border/50 pt-1.5">
          → {empresa.proxima_acao_descricao}
        </p>
      )}

      {/* Botão mover */}
      <div className="mt-2 pt-2 border-t border-border/50">
        {showMove ? (
          <select
            className="select text-[10px] py-0.5"
            defaultValue=""
            onChange={e => { if (e.target.value) { onMove(e.target.value as PipelineStage); setShowMove(false) } }}
          >
            <option value="" disabled>Mover para…</option>
            {STAGES_ORDERED.filter(s => s !== currentStage).map(s => (
              <option key={s} value={s}>{STAGE_LABELS[s]}</option>
            ))}
          </select>
        ) : (
          <button
            onClick={() => setShowMove(true)}
            className="flex items-center gap-1 text-text-faint hover:text-vf-red-light text-[10px] transition-colors"
          >
            <ChevronRight className="w-3 h-3" /> Mover estágio
          </button>
        )}
      </div>
    </div>
  )
}
