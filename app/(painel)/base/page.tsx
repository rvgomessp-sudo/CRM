'use client'
// app/(painel)/base/page.tsx
import { useEffect, useState, useCallback } from 'react'
import Link from 'next/link'
import { createClient } from '@/lib/supabase/client'
import { Empresa } from '@/lib/types'
import { formatBRL } from '@/lib/utils'

const PAGE_SIZE = 50

export default function BasePGFNPage() {
  const [empresas, setEmpresas] = useState<Empresa[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(0)
  const [busca, setBusca] = useState('')
  const [filtroUF, setFiltroUF] = useState('todos')
  const [filtroPrioridade, setFiltroPrioridade] = useState('todos')
  const [filtroSeg, setFiltroSeg] = useState('todos')
  const [loading, setLoading] = useState(true)
  const supabase = createClient()

  const carregar = useCallback(async () => {
    setLoading(true)
    let query = supabase
      .from('empresas')
      .select('*', { count: 'exact' })
      .eq('excluido', false)

    if (busca) {
      query = query.or(`nome_devedor.ilike.%${busca}%,cnpj_raiz.ilike.%${busca}%,cnpj_completo.ilike.%${busca}%`)
    }
    if (filtroUF !== 'todos') query = query.eq('uf_devedor', filtroUF)
    if (filtroPrioridade !== 'todos') query = query.eq('prioridade', filtroPrioridade)
    if (filtroSeg !== 'todos') query = query.eq('seguradora', filtroSeg)

    const { data, count } = await query
      .order('prioridade', { ascending: true })
      .order('valor_total_brl', { ascending: false })
      .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1)

    setEmpresas(data || [])
    setTotal(count || 0)
    setLoading(false)
  }, [busca, filtroUF, filtroPrioridade, filtroSeg, page])

  useEffect(() => { carregar() }, [carregar])
  useEffect(() => { setPage(0) }, [busca, filtroUF, filtroPrioridade, filtroSeg])

  const totalPages = Math.ceil(total / PAGE_SIZE)

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20 }}>
        <div>
          <h1 style={{ fontSize: 20, fontWeight: 700, marginBottom: 2 }}>Base PGFN</h1>
          <p style={{ color: 'var(--color-text-muted)', fontSize: 13 }}>{total} empresas na base F2</p>
        </div>
        <Link href="/importar" className="btn btn-primary" style={{ textDecoration: 'none' }}>
          ↑ Importar CSV
        </Link>
      </div>

      {/* Filtros */}
      <div style={{ display: 'flex', gap: 10, marginBottom: 16, flexWrap: 'wrap' }}>
        <input
          type="text"
          placeholder="Buscar empresa ou CNPJ..."
          value={busca}
          onChange={e => setBusca(e.target.value)}
          style={{ flex: 1, minWidth: 200 }}
        />
        <select value={filtroPrioridade} onChange={e => setFiltroPrioridade(e.target.value)} style={{ width: 'auto' }}>
          <option value="todos">Todas prioridades</option>
          <option value="ALTA">Alta</option>
          <option value="MEDIA">Média</option>
          <option value="BAIXA">Baixa</option>
        </select>
        <select value={filtroSeg} onChange={e => setFiltroSeg(e.target.value)} style={{ width: 'auto' }}>
          <option value="todos">Todas seguradoras</option>
          <option value="Sancor">Sancor</option>
          <option value="Berkley">Berkley</option>
          <option value="Zurich">Zurich</option>
        </select>
        <select value={filtroUF} onChange={e => setFiltroUF(e.target.value)} style={{ width: 'auto' }}>
          <option value="todos">Todas UFs</option>
          <option value="SP">SP</option>
          <option value="MS">MS</option>
        </select>
      </div>

      {/* Tabela */}
      <div className="card" style={{ padding: 0, overflowX: 'auto' }}>
        <table>
          <thead>
            <tr>
              <th>Empresa</th>
              <th>CNPJ</th>
              <th>UF</th>
              <th>Prioridade</th>
              <th>Seguradora</th>
              <th>Inscrições</th>
              <th>Valor Total</th>
              <th>Estágio</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr><td colSpan={8} style={{ textAlign: 'center', padding: 32, color: 'var(--color-text-muted)' }}>Carregando...</td></tr>
            ) : empresas.length === 0 ? (
              <tr><td colSpan={8} style={{ textAlign: 'center', padding: 32, color: 'var(--color-text-muted)' }}>Nenhum resultado</td></tr>
            ) : empresas.map(e => (
              <tr key={e.cnpj_raiz}>
                <td>
                  <Link href={`/empresa/${e.cnpj_raiz}`} style={{ color: '#60a5fa', textDecoration: 'none', fontWeight: 500 }}>
                    {e.nome_devedor}
                  </Link>
                </td>
                <td style={{ fontFamily: 'monospace', fontSize: 12 }}>{e.cnpj_completo || e.cnpj_raiz}</td>
                <td>{e.uf_devedor}</td>
                <td>
                  <span className={`badge badge-${(e.prioridade || 'baixa').toLowerCase()}`}>
                    {e.prioridade}
                  </span>
                </td>
                <td>
                  <span className={`badge badge-${(e.seguradora || '').toLowerCase().replace(/\s/g, '')}`}>
                    {e.seguradora}
                  </span>
                </td>
                <td style={{ textAlign: 'center' }}>{e.qtd_inscricoes_empresa}</td>
                <td style={{ fontWeight: 600 }}>{formatBRL(e.valor_total_brl)}</td>
                <td style={{ fontSize: 12, color: 'var(--color-text-muted)' }}>
                  {e.estagio.replace(/_/g, ' ')}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Paginação */}
      {totalPages > 1 && (
        <div style={{ display: 'flex', justifyContent: 'center', gap: 8, marginTop: 16 }}>
          <button
            className="btn btn-ghost"
            onClick={() => setPage(p => Math.max(0, p - 1))}
            disabled={page === 0}
          >← Anterior</button>
          <span style={{ padding: '6px 14px', color: 'var(--color-text-muted)', fontSize: 13 }}>
            Página {page + 1} de {totalPages}
          </span>
          <button
            className="btn btn-ghost"
            onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
            disabled={page >= totalPages - 1}
          >Próxima →</button>
        </div>
      )}
    </div>
  )
}
