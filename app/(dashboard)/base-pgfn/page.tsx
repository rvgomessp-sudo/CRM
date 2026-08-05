'use client'

import { useEffect, useState, useCallback } from 'react'
import Link from 'next/link'
import { createClient } from '@/lib/supabase/client'
import { formatBRLCompact, formatDate, cn } from '@/lib/utils'
import { MOTOR_COLORS, STAGE_LABELS, type Empresa, type PipelineStage, type MotorTipo } from '@/lib/types'
import { Search, Filter, ChevronLeft, ChevronRight, Building2 } from 'lucide-react'

const PAGE_SIZE = 50

export default function BasePGFNPage() {
  const supabase = createClient()

  const [empresas, setEmpresas] = useState<Empresa[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(0)
  const [loading, setLoading] = useState(true)
  const [search, setSearch] = useState('')
  const [debouncedSearch, setDebouncedSearch] = useState('')
  const [filters, setFilters] = useState({
    motor: '', estagio: '', prioridade: '', uf: '', seguradora: ''
  })

  // Debounce da busca
  useEffect(() => {
    const timer = setTimeout(() => { setDebouncedSearch(search); setPage(0) }, 400)
    return () => clearTimeout(timer)
  }, [search])

  const fetchData = useCallback(async () => {
    setLoading(true)

    let query = supabase
      .from('empresas')
      .select('*', { count: 'exact' })
      .eq('ativo', true)
      .eq('excluido', false)

    // Busca textual
    if (debouncedSearch) {
      query = query.ilike('nome_devedor', `%${debouncedSearch}%`)
    }

    // Filtros
    if (filters.motor)      query = query.eq('motor', filters.motor)
    if (filters.estagio)    query = query.eq('estagio', filters.estagio)
    if (filters.prioridade) query = query.eq('prioridade', filters.prioridade)
    if (filters.uf)         query = query.eq('uf_devedor', filters.uf)
    if (filters.seguradora) query = query.eq('seguradora_alvo', filters.seguradora)

    // Paginação
    const from = page * PAGE_SIZE
    const to = from + PAGE_SIZE - 1

    const { data, count, error } = await query
      .order('valor_total_devida', { ascending: false })
      .range(from, to)

    if (!error) {
      setEmpresas(data || [])
      setTotal(count || 0)
    }
    setLoading(false)
  }, [debouncedSearch, filters, page])

  useEffect(() => { fetchData() }, [fetchData])

  function updateFilter(key: string, value: string) {
    setFilters(prev => ({ ...prev, [key]: value }))
    setPage(0)
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
            {['A1','A2','B1','B2'].map(m => <option key={m} value={m}>{m}</option>)}
          </select>

          <select className="select py-1.5 text-xs w-auto" value={filters.estagio} onChange={e => updateFilter('estagio', e.target.value)}>
            <option value="">Estágio</option>
            {Object.entries(STAGE_LABELS).map(([k, v]) => <option key={k} value={k}>{v}</option>)}
          </select>

          <select className="select py-1.5 text-xs w-auto" value={filters.prioridade} onChange={e => updateFilter('prioridade', e.target.value)}>
            <option value="">Prioridade</option>
            {['ALTA','MEDIA','BAIXA'].map(p => <option key={p} value={p}>{p}</option>)}
          </select>

          <select className="select py-1.5 text-xs w-auto" value={filters.uf} onChange={e => updateFilter('uf', e.target.value)}>
            <option value="">UF</option>
            {['SP','MS'].map(u => <option key={u} value={u}>{u}</option>)}
          </select>

          <select className="select py-1.5 text-xs w-auto" value={filters.seguradora} onChange={e => updateFilter('seguradora', e.target.value)}>
            <option value="">Seguradora</option>
            {['SANCOR','BERKLEY','ZURICH','SWISS','CHUBB'].map(s => <option key={s} value={s}>{s}</option>)}
          </select>

          {Object.values(filters).some(Boolean) && (
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
              <th>Empresa</th>
              <th>UF</th>
              <th>Motor</th>
              <th>Inscrições</th>
              <th className="text-right">Dívida Total</th>
              <th>Estágio</th>
              <th>Seguradora</th>
              <th>Prioridade</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={8} className="text-center text-text-faint py-12">Carregando…</td>
              </tr>
            ) : empresas.length === 0 ? (
              <tr>
                <td colSpan={8} className="text-center text-text-faint py-12">
                  <Building2 className="w-8 h-8 mx-auto mb-2 opacity-30" />
                  Nenhuma empresa encontrada
                </td>
              </tr>
            ) : (
              empresas.map(emp => (
                <tr key={emp.cnpj_raiz} className="group">
                  <td>
                    <Link
                      href={`/empresa/${emp.cnpj_raiz}`}
                      className="text-text-primary group-hover:text-vf-red-light font-medium"
                    >
                      {emp.nome_devedor}
                    </Link>
                    <p className="text-text-faint text-[10px]">{emp.cnpj_completo}</p>
                  </td>
                  <td className="text-text-muted">{emp.uf_devedor || '—'}</td>
                  <td>
                    {emp.motor ? (
                      <span className={cn('badge text-[10px]', MOTOR_COLORS[emp.motor as MotorTipo])}>
                        {emp.motor}
                      </span>
                    ) : '—'}
                  </td>
                  <td className="text-text-muted">{emp.qtd_inscricoes}</td>
                  <td className="text-right font-semibold text-text-primary">
                    {formatBRLCompact(emp.valor_total_devida)}
                  </td>
                  <td>
                    <span className="text-text-muted text-xs">
                      {STAGE_LABELS[emp.estagio as PipelineStage] || emp.estagio}
                    </span>
                  </td>
                  <td>
                    <span className="text-text-muted text-xs">{emp.seguradora_alvo}</span>
                  </td>
                  <td>
                    {emp.prioridade && (
                      <span className={cn('badge text-[10px]',
                        emp.prioridade === 'ALTA' ? 'bg-red-500/20 text-red-400' :
                        emp.prioridade === 'MEDIA' ? 'bg-amber-500/20 text-amber-400' :
                        'bg-text-faint/20 text-text-faint'
                      )}>
                        {emp.prioridade}
                      </span>
                    )}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Paginação */}
      <div className="px-6 py-3 border-t border-border bg-bg-secondary flex items-center justify-between text-xs text-text-muted">
        <span>
          {page * PAGE_SIZE + 1}–{Math.min((page + 1) * PAGE_SIZE, total)} de {total} empresas
        </span>
        <div className="flex gap-1">
          <button
            onClick={() => setPage(p => Math.max(0, p - 1))}
            disabled={page === 0}
            className="btn-ghost py-1 disabled:opacity-40"
          >
            <ChevronLeft className="w-3.5 h-3.5" />
          </button>
          <span className="px-2 py-1">Página {page + 1} / {totalPages || 1}</span>
          <button
            onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
            disabled={page >= totalPages - 1}
            className="btn-ghost py-1 disabled:opacity-40"
          >
            <ChevronRight className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>
    </div>
  )
}
