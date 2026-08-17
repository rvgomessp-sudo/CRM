'use client'

import { Suspense, useCallback, useEffect, useState } from 'react'
import { useRouter, useSearchParams } from 'next/navigation'
import Link from 'next/link'
import { createClient } from '@/lib/supabase/client'
import { formatBRLCompact, cn } from '@/lib/utils'
import {
  MOTOR_COLORS, STAGE_LABELS, TRIAGEM_COLORS, TRIAGEM_LABELS, FONTE_LABELS,
  EVENTO_JUDICIAL_LABELS, EVENTO_ALERTA_MAXIMO, ZONA_LABELS, ZONA_COLORS,
  type FilaRow, type MotorTipo, type PipelineStage, type TriagemStatus,
} from '@/lib/types'
import {
  Search, Target, Eye, X, Check, ChevronLeft, ChevronRight,
  Building2, RotateCcw, Flame, Gavel, AlertTriangle,
} from 'lucide-react'

// dias desde uma data ISO
function diasDe(d: string | null): number | null {
  if (!d) return null
  return Math.floor((Date.now() - new Date(d).getTime()) / 86400000)
}

const PAGE_SIZE = 50
const UFS = ['AC','AL','AM','AP','BA','CE','DF','ES','GO','MA','MG','MS','MT','PA','PB','PE','PI','PR','RJ','RN','RO','RR','RS','SC','SE','SP','TO']
const MOTORES = ['A1','A2','B1','B2','B3','B4','B5']

// ============================================================
// FILA PRIORIZADA DE OPORTUNIDADES
// Estado dos filtros vive na URL → voltar da ficha preserva tudo.
// ============================================================
function FilaOportunidades() {
  const supabase = createClient()
  const router = useRouter()
  const sp = useSearchParams()

  // ---- estado derivado da URL (fonte única da verdade) ----
  const q         = sp.get('q')        ?? ''
  const motor     = sp.get('motor')    ?? ''
  const uf        = sp.get('uf')       ?? ''
  const triagem   = sp.get('triagem')  ?? ''
  const soAlvo    = sp.get('alvo')     !== '0'   // marinheiro LIGADO por padrão
  const verDescart= sp.get('descart')  === '1'
  const page      = Math.max(0, parseInt(sp.get('page') ?? '0', 10) || 0)

  const [rows, setRows]       = useState<FilaRow[]>([])
  const [total, setTotal]     = useState(0)
  const [loading, setLoading] = useState(true)
  const [busca, setBusca]     = useState(q)

  // atualiza a URL sem recarregar a página
  const setParam = useCallback((patch: Record<string, string | null>) => {
    const next = new URLSearchParams(Array.from(sp.entries()))
    for (const [k, v] of Object.entries(patch)) {
      if (v === null || v === '') next.delete(k)
      else next.set(k, v)
    }
    // qualquer mudança de filtro volta para a primeira página
    if (!('page' in patch)) next.delete('page')
    router.replace(`/oportunidades?${next.toString()}`, { scroll: false })
  }, [router, sp])

  // debounce da busca textual
  useEffect(() => {
    const t = setTimeout(() => { if (busca !== q) setParam({ q: busca || null }) }, 400)
    return () => clearTimeout(t)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [busca])

  const fetchData = useCallback(async () => {
    setLoading(true)
    let query = supabase.from('vw_fila_oportunidades').select('*', { count: 'exact' })

    if (q)       query = query.ilike('nome_devedor', `%${q}%`)
    if (motor)   query = query.eq('motor', motor)
    if (uf)      query = query.eq('uf_devedor', uf)
    if (soAlvo)  query = query.eq('alvo_marinheiro', true)
    if (triagem) query = query.eq('triagem', triagem)
    else if (!verDescart) query = query.neq('triagem', 'DESCARTADO')

    // ordenação explícita — não depender do ORDER BY interno da view na paginação
    query = query
      .order('score', { ascending: false, nullsFirst: false })
      .order('valor_total_devida', { ascending: false })

    const from = page * PAGE_SIZE
    const { data, count, error } = await query.range(from, from + PAGE_SIZE - 1)
    if (!error) { setRows((data as FilaRow[]) || []); setTotal(count || 0) }
    setLoading(false)
  }, [supabase, q, motor, uf, soAlvo, triagem, verDescart, page])

  useEffect(() => { fetchData() }, [fetchData])

  // ---- triagem: grava e a fila lembra amanhã ----
  async function triar(row: FilaRow, novo: TriagemStatus) {
    // otimista
    setRows(rs => rs.map(r => r.oportunidade_id === row.oportunidade_id ? { ...r, triagem: novo } : r))
    const { data: { user } } = await supabase.auth.getUser()
    await supabase.from('oportunidades').update({
      triagem: novo,
      triado_por: user?.id ?? null,
      triado_em: new Date().toISOString(),
      atualizado_em: new Date().toISOString(),
    }).eq('id', row.oportunidade_id)
    // se descartou e não estamos vendo descartados, some da lista
    if (novo === 'DESCARTADO' && !verDescart && triagem !== 'DESCARTADO') {
      setRows(rs => rs.filter(r => r.oportunidade_id !== row.oportunidade_id))
      setTotal(t => Math.max(0, t - 1))
    }
  }

  const totalPages = Math.ceil(total / PAGE_SIZE)
  const filtrosAtivos = !!(q || motor || uf || triagem || soAlvo || verDescart)

  return (
    <div className="flex flex-col h-screen">
      {/* Header */}
      <div className="px-6 py-4 border-b border-border bg-bg-secondary">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <Target className="w-4 h-4 text-vf-red-light" />
            <h1 className="text-text-primary font-semibold">Oportunidades</h1>
            <span className="badge bg-bg-primary text-text-muted">{total} na fila</span>
          </div>
          <p className="text-text-faint text-xs">Risco fiscal · melhores primeiro — trabalhista separado</p>
        </div>

        {/* Filtros (persistem na URL) */}
        <div className="flex flex-wrap gap-2">
          <div className="relative">
            <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-text-faint" />
            <input
              className="input pl-8 py-1.5 text-xs w-64"
              placeholder="Buscar por nome da empresa…"
              value={busca}
              onChange={e => setBusca(e.target.value)}
            />
          </div>

          <select className="select py-1.5 text-xs w-auto" value={motor} onChange={e => setParam({ motor: e.target.value })}>
            <option value="">Motor</option>
            {MOTORES.map(m => <option key={m} value={m}>{m}</option>)}
          </select>

          <select className="select py-1.5 text-xs w-auto" value={uf} onChange={e => setParam({ uf: e.target.value })}>
            <option value="">UF</option>
            {UFS.map(u => <option key={u} value={u}>{u}</option>)}
          </select>

          <select className="select py-1.5 text-xs w-auto" value={triagem} onChange={e => setParam({ triagem: e.target.value })}>
            <option value="">Triagem</option>
            {(['NOVO','VISTO','ABORDAR','DESCARTADO'] as TriagemStatus[]).map(t =>
              <option key={t} value={t}>{TRIAGEM_LABELS[t]}</option>)}
          </select>

          <button
            onClick={() => setParam({ alvo: soAlvo ? '0' : null })}
            className={cn('btn-ghost py-1.5 text-xs', soAlvo && 'bg-vf-red/10 text-vf-red-light')}
          >
            <Flame className="w-3.5 h-3.5" /> Só marinheiro
          </button>

          <button
            onClick={() => setParam({ descart: verDescart ? null : '1' })}
            className={cn('btn-ghost py-1.5 text-xs', verDescart && 'bg-bg-hover text-text-primary')}
          >
            <RotateCcw className="w-3.5 h-3.5" /> Ver descartados
          </button>

          {filtrosAtivos && (
            <button
              onClick={() => { setBusca(''); router.replace('/oportunidades', { scroll: false }) }}
              className="btn-ghost py-1.5 text-xs text-danger hover:text-danger"
            >
              Limpar
            </button>
          )}
        </div>
      </div>

      {/* Tabela */}
      <div className="flex-1 overflow-auto">
        <table className="table-vf">
          <thead className="sticky top-0 bg-bg-secondary z-10">
            <tr>
              <th className="text-right w-14">Score</th>
              <th>Empresa</th>
              <th>UF</th>
              <th>Motor</th>
              <th>Tributo</th>
              <th className="text-right">Dívida</th>
              <th className="text-right">Capital</th>
              <th className="text-right" title="Dívida ÷ Capital — quanto menor, mais segurável">D/C</th>
              <th>Zona</th>
              <th>Evento fiscal</th>
              <th>Insc.</th>
              <th>Estágio</th>
              <th>Triagem</th>
              <th className="text-right">Ações</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr><td colSpan={14} className="text-center text-text-faint py-12">Carregando…</td></tr>
            ) : rows.length === 0 ? (
              <tr><td colSpan={14} className="text-center text-text-faint py-12">
                <Building2 className="w-8 h-8 mx-auto mb-2 opacity-30" />
                Nenhuma oportunidade com esses filtros
              </td></tr>
            ) : rows.map(row => (
              <tr key={row.oportunidade_id} className="group">
                {/* Score */}
                <td className="text-right">
                  <span className={cn('inline-flex items-center justify-center w-9 h-6 rounded font-bold text-xs',
                    (row.score ?? 0) >= 80 ? 'bg-vf-red/25 text-vf-red-light' :
                    (row.score ?? 0) >= 55 ? 'bg-amber-500/20 text-amber-400' :
                    'bg-bg-hover text-text-muted'
                  )}>
                    {row.score != null ? Math.round(row.score) : '—'}
                  </span>
                </td>
                {/* Empresa */}
                <td>
                  <div className="flex items-center gap-1.5">
                    <Link href={`/empresa/${row.cnpj_raiz}`}
                      className="text-text-primary group-hover:text-vf-red-light font-medium">
                      {row.nome_devedor}
                    </Link>
                    {row.alvo_marinheiro && <Flame className="w-3 h-3 text-vf-red-light flex-shrink-0" />}
                  </div>
                  <p className="text-text-faint text-[10px]">
                    {row.cnpj_completo} · {FONTE_LABELS[row.fonte]}
                    {row.cnpj_situacao && row.cnpj_situacao !== 'ATIVA' && (
                      <span className="text-danger"> · {row.cnpj_situacao}</span>
                    )}
                  </p>
                </td>
                <td className="text-text-muted">{row.uf_devedor || '—'}</td>
                <td>{row.motor
                  ? <span className={cn('badge text-[10px]', MOTOR_COLORS[row.motor as MotorTipo])}>{row.motor}</span>
                  : '—'}</td>
                <td className="text-text-muted text-[11px]">{row.tributo_principal ?? '—'}</td>
                <td className="text-right font-semibold text-text-primary">{formatBRLCompact(row.valor_total_devida)}</td>
                <td className="text-right text-text-muted">{formatBRLCompact(row.capital_social)}</td>
                <td className="text-right">
                  {row.ratio_divida_capital != null ? (
                    <span className={cn('font-medium text-xs',
                      row.ratio_divida_capital <= 2 ? 'text-success' :
                      row.ratio_divida_capital <= 5 ? 'text-warning' : 'text-danger')}
                      title="Dívida ÷ Capital">
                      {row.ratio_divida_capital}×
                    </span>
                  ) : <span className="text-text-faint text-xs">—</span>}
                </td>
                {/* Zona de risco */}
                <td>
                  {row.zona_risco ? (
                    <span className={cn('badge text-[10px]', ZONA_COLORS[row.zona_risco] ?? 'bg-bg-hover text-text-muted')}>
                      {row.zona_risco === 'SUFOCO' && '🚨 '}{ZONA_LABELS[row.zona_risco] ?? row.zona_risco}
                    </span>
                  ) : <span className="text-text-faint text-xs">—</span>}
                </td>
                {/* Evento fiscal (trabalhista separado, sem relação securitária) */}
                <td>
                  {row.evento_judicial_tipo ? (() => {
                    const d = diasDe(row.evento_judicial_em)
                    const fresco = d != null && d <= 30
                    const alerta = EVENTO_ALERTA_MAXIMO.has(row.evento_judicial_tipo)
                    const Icon = alerta ? AlertTriangle : Gavel
                    return (
                      <div className={cn('flex items-center gap-1', alerta && 'px-1.5 py-0.5 rounded bg-danger/15 -ml-1.5')}>
                        <Icon className={cn('w-3 h-3 flex-shrink-0',
                          alerta ? 'text-danger' : fresco ? 'text-vf-red-light' : 'text-text-faint')} />
                        <span className={cn('text-[11px]',
                          alerta ? 'text-danger font-semibold' : fresco ? 'text-text-primary' : 'text-text-muted')}>
                          {EVENTO_JUDICIAL_LABELS[row.evento_judicial_tipo] ?? row.evento_judicial_tipo}
                          {d != null && <span className={alerta ? 'text-danger/70' : 'text-text-faint'}> · {d === 0 ? 'hoje' : `${d}d`}</span>}
                        </span>
                      </div>
                    )
                  })() : <span className="text-text-faint text-xs">—</span>}
                  {row.eventos_trabalhistas ? (
                    <span className="block text-[10px] text-text-faint mt-0.5" title="Constrições trabalhistas — sem relação securitária tributária; sinal de estresse de caixa">
                      ⚒ {row.eventos_trabalhistas} trabalhista{row.eventos_trabalhistas > 1 ? 's' : ''}
                    </span>
                  ) : null}
                </td>
                <td className="text-text-muted text-center">{row.qtd_inscricoes}</td>
                <td><span className="text-text-muted text-xs">{STAGE_LABELS[row.estagio as PipelineStage] || row.estagio}</span></td>
                <td><span className={cn('badge text-[10px]', TRIAGEM_COLORS[row.triagem])}>{TRIAGEM_LABELS[row.triagem]}</span></td>
                {/* Ações de triagem */}
                <td>
                  <div className="flex items-center justify-end gap-1 opacity-60 group-hover:opacity-100 transition-opacity">
                    <button title="Marcar como visto" onClick={() => triar(row, 'VISTO')}
                      className="p-1 rounded hover:bg-bg-hover text-text-muted hover:text-text-primary"><Eye className="w-3.5 h-3.5" /></button>
                    <button title="Marcar para abordar" onClick={() => triar(row, 'ABORDAR')}
                      className="p-1 rounded hover:bg-success/15 text-text-muted hover:text-success"><Check className="w-3.5 h-3.5" /></button>
                    <button title="Descartar" onClick={() => triar(row, 'DESCARTADO')}
                      className="p-1 rounded hover:bg-danger/15 text-text-muted hover:text-danger"><X className="w-3.5 h-3.5" /></button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Paginação */}
      <div className="px-6 py-3 border-t border-border bg-bg-secondary flex items-center justify-between text-xs text-text-muted">
        <span>{total === 0 ? '0' : page * PAGE_SIZE + 1}–{Math.min((page + 1) * PAGE_SIZE, total)} de {total}</span>
        <div className="flex items-center gap-1">
          <button onClick={() => setParam({ page: String(Math.max(0, page - 1)) })} disabled={page === 0}
            className="btn-ghost py-1 disabled:opacity-40"><ChevronLeft className="w-3.5 h-3.5" /></button>
          <span className="px-2 py-1">Página {page + 1} / {totalPages || 1}</span>
          <button onClick={() => setParam({ page: String(Math.min(totalPages - 1, page + 1)) })} disabled={page >= totalPages - 1}
            className="btn-ghost py-1 disabled:opacity-40"><ChevronRight className="w-3.5 h-3.5" /></button>
        </div>
      </div>
    </div>
  )
}

export default function OportunidadesPage() {
  return (
    <Suspense fallback={<div className="p-6 text-text-faint text-sm">Carregando fila…</div>}>
      <FilaOportunidades />
    </Suspense>
  )
}
