'use client'

/**
 * DECISORES — onde a inteligência vira prospecção.
 * Lista os decisores/interlocutores (semeados do grafo + adicionados à mão)
 * e deixa o operador ALIMENTAR telefone, e-mail e LinkedIn, além de curar a
 * classificação e o poder decisório. Advogado dos autos entra como
 * INFLUENCIADOR_POTENCIAL / poder NÃO_CONFIRMADO — você promove quando confirmar.
 */

import { Suspense, useCallback, useEffect, useMemo, useState } from 'react'
import Link from 'next/link'
import { useRouter, useSearchParams } from 'next/navigation'
import { createClient } from '@/lib/supabase/client'
import { cn, formatCnpj } from '@/lib/utils'
import { Users, Search, Check, Plus, ExternalLink } from 'lucide-react'

interface Decisor {
  id: string
  cnpj_raiz: string
  nome: string
  cargo: string | null
  classificacao: string
  poder_decisorio: string
  telefone: string | null
  email: string | null
  linkedin: string | null
  fonte: string | null
  evidencia_condicao: string | null
}

const CLASSES: Record<string, { label: string; cls: string }> = {
  DECISOR_FINANCEIRO_POTENCIAL:   { label: 'Financeiro',          cls: 'text-success' },
  DECISOR_JURIDICO_POTENCIAL:     { label: 'Jurídico',            cls: 'text-info' },
  PATROCINADOR_INTERNO_POTENCIAL: { label: 'Sócio / Patrocinador', cls: 'text-vf-red-light' },
  INFLUENCIADOR_POTENCIAL:        { label: 'Influenciador',       cls: 'text-warning' },
  NAO_CONFIRMADO:                 { label: 'Não confirmado',      cls: 'text-text-faint' },
}
const PODERES = ['NAO_CONFIRMADO', 'POTENCIAL', 'CONFIRMADO']
const PODER_LABEL: Record<string, string> = { NAO_CONFIRMADO: 'Não conf.', POTENCIAL: 'Potencial', CONFIRMADO: 'Confirmado' }
const PAGE = 40

export default function DecisoresPage() {
  return <Suspense><Inner /></Suspense>
}

function Inner() {
  const supabase = createClient()
  const router = useRouter()
  const sp = useSearchParams()
  const q      = sp.get('q') ?? ''
  const classe = sp.get('classe') ?? ''
  const sem    = sp.get('sem') === '1'          // só sem contato
  const page   = Math.max(0, parseInt(sp.get('page') ?? '0', 10) || 0)

  const [rows, setRows] = useState<Decisor[]>([])
  const [empresas, setEmpresas] = useState<Map<string, string>>(new Map())
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(true)
  const [busca, setBusca] = useState(q)
  const [savedId, setSavedId] = useState<string | null>(null)
  const [kpi, setKpi] = useState({ total: 0, comContato: 0 })

  const setParam = useCallback((patch: Record<string, string | null>) => {
    const next = new URLSearchParams(Array.from(sp.entries()))
    for (const [k, v] of Object.entries(patch)) { if (!v) next.delete(k); else next.set(k, v) }
    if (!('page' in patch)) next.delete('page')
    router.replace(`/decisores?${next.toString()}`, { scroll: false })
  }, [router, sp])

  useEffect(() => {
    const t = setTimeout(() => { if (busca !== q) setParam({ q: busca || null }) }, 400)
    return () => clearTimeout(t)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [busca])

  const fetchData = useCallback(async () => {
    setLoading(true)
    let query = supabase.from('decisores').select('*', { count: 'exact' })
    if (q)      query = query.ilike('nome', `%${q}%`)
    if (classe) query = query.eq('classificacao', classe)
    if (sem)    query = query.is('telefone', null).is('email', null)
    query = query.order('prioridade', { ascending: true }).order('nome', { ascending: true })
    const from = page * PAGE
    const { data, count } = await query.range(from, from + PAGE - 1)
    const list = (data as Decisor[]) ?? []
    setRows(list); setTotal(count ?? 0)

    const cnpjs = [...new Set(list.map(d => d.cnpj_raiz))]
    if (cnpjs.length) {
      const { data: emp } = await supabase.from('empresas').select('cnpj_raiz,nome_devedor').in('cnpj_raiz', cnpjs)
      setEmpresas(new Map(((emp as any[]) ?? []).map(e => [e.cnpj_raiz, e.nome_devedor])))
    }
    // KPIs globais
    const [{ count: tot }, { count: cc }] = await Promise.all([
      supabase.from('decisores').select('id', { count: 'exact', head: true }),
      supabase.from('decisores').select('id', { count: 'exact', head: true }).or('telefone.not.is.null,email.not.is.null'),
    ])
    setKpi({ total: tot ?? 0, comContato: cc ?? 0 })
    setLoading(false)
  }, [supabase, q, classe, sem, page])
  useEffect(() => { fetchData() }, [fetchData])

  async function save(id: string, patch: Partial<Decisor>) {
    setRows(prev => prev.map(r => r.id === id ? { ...r, ...patch } : r))
    const { error } = await supabase.from('decisores').update(patch).eq('id', id)
    if (!error) { setSavedId(id); setTimeout(() => setSavedId(s => s === id ? null : s), 1500) }
  }

  const totalPages = Math.ceil(total / PAGE)

  return (
    <div className="p-4 sm:p-6 max-w-7xl mx-auto w-full">
      <div className="flex items-center justify-between flex-wrap gap-2 mb-1">
        <h1 className="text-xl font-bold text-text-primary flex items-center gap-2">
          <Users className="w-5 h-5 text-vf-red-light" /> Decisores &amp; contatos
        </h1>
        <div className="text-xs text-text-muted">
          <b className="text-text-primary tabular-nums">{kpi.comContato}</b> com contato ·{' '}
          <b className="text-warning tabular-nums">{kpi.total - kpi.comContato}</b> a enriquecer · {kpi.total} no total
        </div>
      </div>
      <p className="text-text-muted text-sm mb-4">
        Alimente telefone, e-mail e LinkedIn dos decisores. Advogado dos autos entra como interlocutor —
        promova a decisor quando confirmar. Contato aqui não vira fato: fica com a fonte e a condição de evidência.
      </p>

      {/* filtros */}
      <div className="flex flex-wrap gap-2 mb-3">
        <div className="relative">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-text-faint" />
          <input className="input pl-8 py-1.5 text-xs w-56" placeholder="Buscar por nome…"
            value={busca} onChange={e => setBusca(e.target.value)} />
        </div>
        <select className="select py-1.5 text-xs w-auto" value={classe} onChange={e => setParam({ classe: e.target.value })}>
          <option value="">Classificação</option>
          {Object.entries(CLASSES).map(([k, v]) => <option key={k} value={k}>{v.label}</option>)}
        </select>
        <button onClick={() => setParam({ sem: sem ? null : '1' })}
          className={cn('btn-ghost py-1.5 text-xs', sem && 'bg-vf-red/10 text-vf-red-light')}>
          Só sem contato
        </button>
        <span className="text-text-faint text-xs self-center">{total} decisores</span>
      </div>

      {/* tabela editável */}
      <div className="overflow-x-auto border border-border rounded-lg">
        <table className="w-full text-sm min-w-[900px]">
          <thead><tr className="bg-bg-secondary">
            {['Empresa', 'Nome', 'Classificação', 'Poder', 'Telefone', 'E-mail', 'LinkedIn', 'Fonte'].map(h => (
              <th key={h} className="text-left text-[10px] uppercase tracking-wider text-text-faint font-semibold px-3 py-2 border-b border-border-strong">{h}</th>
            ))}
          </tr></thead>
          <tbody>
            {loading ? (
              <tr><td colSpan={8} className="px-3 py-8 text-center text-text-muted text-xs">Carregando…</td></tr>
            ) : rows.length === 0 ? (
              <tr><td colSpan={8} className="px-3 py-8 text-center text-text-muted text-xs">Nenhum decisor com esses filtros.</td></tr>
            ) : rows.map(d => (
              <tr key={d.id} className={cn('border-b border-border/60', savedId === d.id && 'bg-success/5')}>
                <td className="px-3 py-2 max-w-[160px]">
                  <Link href={`/empresa/${d.cnpj_raiz}`} className="text-text-primary hover:text-vf-red-light text-xs block truncate">
                    {empresas.get(d.cnpj_raiz) ?? formatCnpj(d.cnpj_raiz)}
                  </Link>
                </td>
                <td className="px-3 py-2 max-w-[170px]">
                  <span className="text-text-primary text-xs block truncate" title={d.nome}>{d.nome}</span>
                  {d.cargo && <span className="text-text-faint text-[10px] block truncate">{d.cargo}</span>}
                </td>
                <td className="px-3 py-2">
                  <select value={d.classificacao} onChange={e => save(d.id, { classificacao: e.target.value })}
                    className={cn('bg-transparent text-xs border border-border rounded px-1.5 py-1 focus:border-vf-red outline-none', CLASSES[d.classificacao]?.cls)}>
                    {Object.entries(CLASSES).map(([k, v]) => <option key={k} value={k} className="bg-bg-card text-text-primary">{v.label}</option>)}
                  </select>
                </td>
                <td className="px-3 py-2">
                  <select value={d.poder_decisorio} onChange={e => save(d.id, { poder_decisorio: e.target.value })}
                    className="bg-transparent text-xs border border-border rounded px-1.5 py-1 focus:border-vf-red outline-none text-text-muted">
                    {PODERES.map(p => <option key={p} value={p} className="bg-bg-card text-text-primary">{PODER_LABEL[p]}</option>)}
                  </select>
                </td>
                <td className="px-3 py-2">
                  <input defaultValue={d.telefone ?? ''} placeholder="—" onBlur={e => e.target.value !== (d.telefone ?? '') && save(d.id, { telefone: e.target.value || null })}
                    className="bg-transparent text-xs border border-border rounded px-2 py-1 w-28 focus:border-vf-red outline-none text-text-primary font-mono" />
                </td>
                <td className="px-3 py-2">
                  <input defaultValue={d.email ?? ''} placeholder="—" onBlur={e => e.target.value !== (d.email ?? '') && save(d.id, { email: e.target.value || null })}
                    className="bg-transparent text-xs border border-border rounded px-2 py-1 w-44 focus:border-vf-red outline-none text-text-primary" />
                </td>
                <td className="px-3 py-2">
                  <input defaultValue={d.linkedin ?? ''} placeholder="—" onBlur={e => e.target.value !== (d.linkedin ?? '') && save(d.id, { linkedin: e.target.value || null })}
                    className="bg-transparent text-xs border border-border rounded px-2 py-1 w-32 focus:border-vf-red outline-none text-info" />
                </td>
                <td className="px-3 py-2">
                  <span className="text-[10px] text-text-faint">{d.fonte === 'DJEN_COMUNICA' ? 'Advogado (DJEN)' : d.fonte === 'GRAPH_PROJECTION' ? 'Grafo' : d.fonte ?? '—'}</span>
                  {savedId === d.id && <Check className="w-3 h-3 text-success inline ml-1" />}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* paginação */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between mt-3 text-xs">
          <span className="text-text-faint">Página {page + 1} de {totalPages}</span>
          <div className="flex gap-2">
            <button disabled={page === 0} onClick={() => setParam({ page: String(page - 1) })}
              className="btn-secondary py-1 px-3 disabled:opacity-40">Anterior</button>
            <button disabled={page + 1 >= totalPages} onClick={() => setParam({ page: String(page + 1) })}
              className="btn-secondary py-1 px-3 disabled:opacity-40">Próxima</button>
          </div>
        </div>
      )}

      <p className="text-[11px] text-text-faint mt-6 border-t border-border pt-4 max-w-3xl">
        O dossiê de inteligência (OSINT/grafo) é gerado pelo motor VF Graph Intelligence, que roda separado.
        Quando ele processa uma empresa, os decisores e evidências entram aqui automaticamente. Enquanto o
        motor não está plugado, você alimenta os contatos manualmente nesta tela.
      </p>
    </div>
  )
}
