'use client'

import { useEffect, useState, useCallback } from 'react'
import Link from 'next/link'
import { createClient } from '@/lib/supabase/client'
import { formatBRLCompact, cn } from '@/lib/utils'
import {
  MOTOR_COLORS, STAGE_LABELS, STAGE_COLORS, STAGES_ORDERED, STAGE_DB_VALUES, normalizeStage,
  type FilaRow, type PipelineStage, type MotorTipo,
} from '@/lib/types'
import { ScoreBadge, ZonaBadge, EventoFiscalCell, MarinheiroFlame } from '@/components/intel'
import { Search, ChevronLeft, ChevronRight, Building2, ArrowRight } from 'lucide-react'

const PAGE_SIZE = 50

// A Base PGFN lê a MESMA fonte da fila de oportunidades (vw_fila_oportunidades):
// score, zona e evento aparecem aqui exatamente como lá — os filtros conversam.
export default function BasePGFNPage() {
  const supabase = createClient()

  const [rows, setRows] = useState<FilaRow[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(0)
  const [loading, setLoading] = useState(true)
  const [promovendo, setPromovendo] = useState<string | null>(null)
  const [search, setSearch] = useState('')
  const [debouncedSearch, setDebouncedSearch] = useState('')
  const [filters, setFilters] = useState({
    motor: '', estagio: '', prioridade: '', uf: '', seguradora: '',
  })

  // Debounce da busca
  useEffect(() => {
    const timer = setTimeout(() => { setDebouncedSearch(search); setPage(0) }, 400)
    return () => clearTimeout(timer)
  }, [search])

  const fetchData = useCallback(async () => {
    setLoading(true)

    let query = supabase
      .from('vw_fila_oportunidades')
      .select('*', { count: 'exact' })

    if (debouncedSearch) query = query.ilike('nome_devedor', `%${debouncedSearch}%`)

    if (filters.motor)      query = query.eq('motor', filters.motor)
    // Filtra pela etapa canônica incluindo valores legados equivalentes.
    if (filters.estagio)    query = query.in('estagio', STAGE_DB_VALUES[filters.estagio as PipelineStage] ?? [filters.estagio])
    if (filters.prioridade) query = query.eq('prioridade', filters.prioridade)
    if (filters.uf)         query = query.eq('uf_devedor', filters.uf)
    if (filters.seguradora) query = query.eq('seguradora_alvo', filters.seguradora)

    const from = page * PAGE_SIZE
    const { data, count, error } = await query
      .order('score', { ascending: false, nullsFirst: false })
      .order('valor_total_devida', { ascending: false })
      .range(from, from + PAGE_SIZE - 1)

    if (!error) {
      setRows((data as FilaRow[]) || [])
      setTotal(count || 0)
    }
    setLoading(false)
  }, [supabase, debouncedSearch, filters, page])

  useEffect(() => { fetchData() }, [fetchData])

  function updateFilter(key: string, value: string) {
    setFilters(prev => ({ ...prev, [key]: value }))
    setPage(0)
  }

  // Promove da base bruta para o funil ativo: Oportunidade → Abordagem.
  async function promover(cnpjRaiz: string) {
    setPromovendo(cnpjRaiz)
    await supabase.from('empresas')
      .update({ estagio: 'abordagem', atualizado_em: new Date().toISOString() })
      .eq('cnpj_raiz', cnpjRaiz)
    await fetchData()
    setPromovendo(null)
  }

  const totalPages = Math.ceil(total / PAGE_SIZE)

  return (
    <div className="flex flex-col h-screen">

      {/* Header */}
      <div className="px-6 py-4 border-b border-border bg-bg-secondary">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <h1 className="text-text-primary font-semibold">Base PGFN</h1>
            <span className="badge bg-bg-primary text-text-muted">{total} empresas</span>
          </div>
          <p className="text-text-faint text-xs">Mesma régua da fila — score, zona e evento em toda linha</p>
        </div>

        {/* Busca + Filtros */}
        <div className="flex flex-wrap gap-2">
          <div className="relative">
            <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-text-faint" />
            <input
              className="input pl-8 py-1.5 text-xs w-64"
              placeholder="Buscar por nome da empresa…"
              value={search}
              onChange={e => setSearch(e.target.value)}
            />
          </div>

          <select className="select py-1.5 text-xs w-auto" value={filters.motor} onChange={e => updateFilter('motor', e.target.value)}>
            <option value="">Motor</option>
            {['A1','A2','B1','B2','B3','B4','B5'].map(m => <option key={m} value={m}>{m}</option>)}
          </select>

          <select className="select py-1.5 text-xs w-auto" value={filters.estagio} onChange={e => updateFilter('estagio', e.target.value)}>
            <option value="">Estágio</option>
            {STAGES_ORDERED.map(s => <option key={s} value={s}>{STAGE_LABELS[s]}</option>)}
          </select>

          <select className="select py-1.5 text-xs w-auto" value={filters.prioridade} onChange={e => updateFilter('prioridade', e.target.value)}>
            <option value="">Prioridade</option>
            {['ALTA','MEDIA','BAIXA'].map(p => <option key={p} value={p}>{p}</option>)}
          </select>

          <select className="select py-1.5 text-xs w-auto" value={filters.uf} onChange={e => updateFilter('uf', e.target.value)}>
            <option value="">UF</option>
            {['AC','AL','AM','AP','BA','CE','DF','ES','GO','MA','MG','MS','MT','PA','PB','PE','PI','PR','RJ','RN','RO','RR','RS','SC','SE','SP','TO'].map(u => <option key={u} value={u}>{u}</option>)}
          </select>

          <select className="select py-1.5 text-xs w-auto" value={filters.seguradora} onChange={e => updateFilter('seguradora', e.target.value)}>
            <option value="">Seguradora</option>
            {['SANCOR','BERKLEY','ZURICH','SWISS','CHUBB'].map(s => <option key={s} value={s}>{s}</option>)}
          </select>

          {(Object.values(filters).some(Boolean) || search) && (
            <button
              onClick={() => { setFilters({ motor:'', estagio:'', prioridade:'', uf:'', seguradora:'' }); setSearch('') }}
              className="btn-ghost py-1.5 text-xs text-danger hover:text-danger"
            >
              Limpar filtros
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
              <th>Zona</th>
              <th>Evento fiscal</th>
              <th className="text-right" title="Dívida ÷ Capital — quanto menor, mais segurável">D/C</th>
              <th className="text-right">Dívida Total</th>
              <th>Insc.</th>
              <th>Estágio</th>
              <th>Seguradora</th>
              <th className="text-right w-20"></th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr><td colSpan={12} className="text-center text-text-faint py-12">Carregando…</td></tr>
            ) : rows.length === 0 ? (
              <tr><td colSpan={12} className="text-center text-text-faint py-12">
                <Building2 className="w-8 h-8 mx-auto mb-2 opacity-30" />
                Nenhuma empresa encontrada
              </td></tr>
            ) : (
              rows.map(row => {
                const canon = normalizeStage(row.estagio)
                return (
                  <tr key={row.cnpj_raiz} className="group">
                    {/* Score */}
                    <td className="text-right"><ScoreBadge score={row.score} /></td>
                    {/* Empresa */}
                    <td>
                      <div className="flex items-center gap-1.5">
                        <Link href={`/empresa/${row.cnpj_raiz}`} className="text-text-primary group-hover:text-vf-red-light font-medium">
                          {row.nome_devedor}
                        </Link>
                        <MarinheiroFlame on={row.alvo_marinheiro} />
                      </div>
                      <p className="text-text-faint text-[10px]">
                        {row.cnpj_completo}
                        {row.cnpj_situacao && row.cnpj_situacao !== 'ATIVA' && (
                          <span className="text-danger"> · {row.cnpj_situacao}</span>
                        )}
                      </p>
                    </td>
                    <td className="text-text-muted">{row.uf_devedor || '—'}</td>
                    <td>
                      {row.motor
                        ? <span className={cn('badge text-[10px]', MOTOR_COLORS[row.motor as MotorTipo])}>{row.motor}</span>
                        : '—'}
                    </td>
                    <td><ZonaBadge zona={row.zona_risco} /></td>
                    <td><EventoFiscalCell tipo={row.evento_judicial_tipo} em={row.evento_judicial_em} trabalhistas={row.eventos_trabalhistas} /></td>
                    <td className="text-right">
                      {row.ratio_divida_capital != null ? (
                        <span className={cn('font-medium text-xs',
                          row.ratio_divida_capital <= 2 ? 'text-success' :
                          row.ratio_divida_capital <= 5 ? 'text-warning' : 'text-danger')} title="Dívida ÷ Capital">
                          {row.ratio_divida_capital}×
                        </span>
                      ) : <span className="text-text-faint text-xs">—</span>}
                    </td>
                    <td className="text-right font-semibold text-text-primary">{formatBRLCompact(row.valor_total_devida)}</td>
                    <td className="text-text-muted text-center">{row.qtd_inscricoes}</td>
                    <td>
                      <span className={cn('badge text-[10px]', STAGE_COLORS[canon])}>{STAGE_LABELS[canon]}</span>
                    </td>
                    <td><span className="text-text-muted text-xs">{row.seguradora_alvo}</span></td>
                    {/* Promover para o funil ativo */}
                    <td className="text-right">
                      {canon === 'oportunidade' && (
                        <button
                          onClick={() => promover(row.cnpj_raiz)}
                          disabled={promovendo === row.cnpj_raiz}
                          title="Promover para Abordagem"
                          className="inline-flex items-center gap-1 text-[10px] text-text-faint hover:text-vf-red-light opacity-0 group-hover:opacity-100 transition-opacity disabled:opacity-50"
                        >
                          {promovendo === row.cnpj_raiz ? '…' : <>Abordar <ArrowRight className="w-3 h-3" /></>}
                        </button>
                      )}
                    </td>
                  </tr>
                )
              })
            )}
          </tbody>
        </table>
      </div>

      {/* Paginação */}
      <div className="px-6 py-3 border-t border-border bg-bg-secondary flex items-center justify-between text-xs text-text-muted">
        <span>{total === 0 ? '0' : page * PAGE_SIZE + 1}–{Math.min((page + 1) * PAGE_SIZE, total)} de {total} empresas</span>
        <div className="flex gap-1">
          <button onClick={() => setPage(p => Math.max(0, p - 1))} disabled={page === 0}
            className="btn-ghost py-1 disabled:opacity-40"><ChevronLeft className="w-3.5 h-3.5" /></button>
          <span className="px-2 py-1">Página {page + 1} / {totalPages || 1}</span>
          <button onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))} disabled={page >= totalPages - 1}
            className="btn-ghost py-1 disabled:opacity-40"><ChevronRight className="w-3.5 h-3.5" /></button>
        </div>
      </div>
    </div>
  )
}
