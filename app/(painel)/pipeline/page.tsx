'use client'
// app/(painel)/pipeline/page.tsx
import { useEffect, useState, useCallback } from 'react'
import Link from 'next/link'
import { createClient } from '@/lib/supabase/client'
import { ESTAGIO_LABELS, ESTAGIO_ORDEM, EstagioPipeline, Empresa } from '@/lib/types'
import { formatBRL, podeProceder } from '@/lib/utils'

const ESTAGIO_CORES: Record<EstagioPipeline, string> = {
  base_pgfn: '#374151',
  enriquecimento: '#1e3a5f',
  abordagem: '#1e40af',
  interesse_manifesto: '#4c1d95',
  analise_rapida: '#7c3aed',
  proposta_enviada: '#92400e',
  submetido_sancor: '#065f46',
  aprovado: '#14532d',
  fechado: '#166534',
  receita_realizada: '#15803d',
}

export default function PipelinePage() {
  const [empresas, setEmpresas] = useState<Empresa[]>([])
  const [loading, setLoading] = useState(true)
  const [filtroResp, setFiltroResp] = useState('todos')
  const [filtroSeg, setFiltroSeg] = useState('todos')
  const supabase = createClient()

  const carregar = useCallback(async () => {
    let query = supabase
      .from('empresas')
      .select('*')
      .eq('excluido', false)
      .order('prioridade', { ascending: true })

    if (filtroResp !== 'todos') query = query.eq('responsavel_id', filtroResp)
    if (filtroSeg !== 'todos') query = query.eq('seguradora', filtroSeg)

    const { data } = await query
    setEmpresas(data || [])
    setLoading(false)
  }, [filtroResp, filtroSeg])

  useEffect(() => { carregar() }, [carregar])

  async function moverEstagio(empresa: Empresa, novoEstagio: EstagioPipeline) {
    const { ok, camposFaltando } = podeProceder(empresa, novoEstagio)
    if (!ok) {
      alert(`Campos obrigatórios faltando: ${camposFaltando.join(', ')}`)
      return
    }
    await supabase
      .from('empresas')
      .update({ estagio: novoEstagio, atualizado_em: new Date().toISOString() })
      .eq('cnpj_raiz', empresa.cnpj_raiz)
    carregar()
  }

  const grupos = ESTAGIO_ORDEM.reduce((acc, estagio) => {
    acc[estagio] = empresas.filter(e => e.estagio === estagio)
    return acc
  }, {} as Record<EstagioPipeline, Empresa[]>)

  if (loading) return <div style={{ padding: 40, color: 'var(--color-text-muted)' }}>Carregando pipeline...</div>

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20 }}>
        <div>
          <h1 style={{ fontSize: 20, fontWeight: 700, marginBottom: 2 }}>Pipeline</h1>
          <p style={{ color: 'var(--color-text-muted)', fontSize: 13 }}>{empresas.length} empresas ativas</p>
        </div>
        <div style={{ display: 'flex', gap: 10 }}>
          <select value={filtroSeg} onChange={e => setFiltroSeg(e.target.value)} style={{ width: 'auto' }}>
            <option value="todos">Todas seguradoras</option>
            <option value="Sancor">Sancor</option>
            <option value="Berkley">Berkley</option>
            <option value="Zurich">Zurich</option>
          </select>
        </div>
      </div>

      {/* Kanban scroll horizontal */}
      <div style={{
        display: 'flex',
        gap: 12,
        overflowX: 'auto',
        paddingBottom: 16,
        minHeight: 'calc(100vh - 140px)',
      }}>
        {ESTAGIO_ORDEM.map((estagio, idx) => {
          const cards = grupos[estagio]
          const valorTotal = cards.reduce((acc, e) => acc + (e.valor_total_brl || 0), 0)
          const cor = ESTAGIO_CORES[estagio]

          return (
            <div key={estagio} style={{
              minWidth: 230,
              maxWidth: 230,
              flexShrink: 0,
              display: 'flex',
              flexDirection: 'column',
            }}>
              {/* Header da coluna */}
              <div style={{
                background: 'var(--color-surface)',
                border: `1px solid var(--color-border)`,
                borderTop: `3px solid ${cor}`,
                borderRadius: 8,
                padding: '10px 12px',
                marginBottom: 8,
              }}>
                <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--color-text-muted)', marginBottom: 2 }}>
                  ETAPA {idx + 1}
                </div>
                <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 4 }}>
                  {ESTAGIO_LABELS[estagio].replace(/^\d+\. /, '')}
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11 }}>
                  <span style={{ color: 'var(--color-text-muted)' }}>{cards.length} empresas</span>
                  <span style={{ fontWeight: 600, color: cor }}>{formatBRL(valorTotal)}</span>
                </div>
              </div>

              {/* Cards */}
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8, flex: 1, overflowY: 'auto' }}>
                {cards.map(empresa => (
                  <KanbanCard
                    key={empresa.cnpj_raiz}
                    empresa={empresa}
                    onMover={moverEstagio}
                    estagioAtual={estagio}
                  />
                ))}
                {cards.length === 0 && (
                  <div style={{
                    border: '1px dashed var(--color-border)',
                    borderRadius: 6,
                    padding: 16,
                    textAlign: 'center',
                    color: 'var(--color-text-muted)',
                    fontSize: 12,
                  }}>
                    Vazio
                  </div>
                )}
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

function KanbanCard({ empresa, onMover, estagioAtual }: {
  empresa: Empresa
  onMover: (e: Empresa, estagio: EstagioPipeline) => void
  estagioAtual: EstagioPipeline
}) {
  const idxAtual = ESTAGIO_ORDEM.indexOf(estagioAtual)
  const proximoEstagio = ESTAGIO_ORDEM[idxAtual + 1]

  const corPrioridade = empresa.prioridade === 'ALTA' ? '#f87171'
    : empresa.prioridade === 'MEDIA' ? '#fbbf24' : '#6b7280'

  return (
    <div style={{
      background: 'var(--color-surface)',
      border: '1px solid var(--color-border)',
      borderLeft: `3px solid ${corPrioridade}`,
      borderRadius: 6,
      padding: '10px 12px',
      cursor: 'pointer',
    }}>
      <Link href={`/empresa/${empresa.cnpj_raiz}`} style={{ textDecoration: 'none', color: 'inherit' }}>
        <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 4, lineHeight: 1.3 }}>
          {empresa.nome_devedor.length > 28
            ? empresa.nome_devedor.substring(0, 28) + '…'
            : empresa.nome_devedor}
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
          <span style={{ fontSize: 11, color: 'var(--color-text-muted)' }}>
            {empresa.cnpj_completo || empresa.cnpj_raiz}
          </span>
          <span style={{ fontSize: 11, fontWeight: 600, color: corPrioridade }}>
            {empresa.prioridade}
          </span>
        </div>
        <div style={{ fontSize: 12, fontWeight: 700, color: '#60a5fa' }}>
          {formatBRL(empresa.valor_total_brl)}
        </div>
        <div style={{ fontSize: 10, color: 'var(--color-text-muted)', marginTop: 2 }}>
          {empresa.seguradora} · {empresa.qtd_inscricoes_empresa} inscr.
        </div>
      </Link>
      {proximoEstagio && (
        <button
          onClick={() => onMover(empresa, proximoEstagio)}
          style={{
            marginTop: 8,
            width: '100%',
            padding: '4px 0',
            background: 'transparent',
            border: '1px solid var(--color-border)',
            borderRadius: 4,
            color: 'var(--color-text-muted)',
            fontSize: 11,
            cursor: 'pointer',
          }}
          onMouseEnter={e => (e.currentTarget.style.color = 'var(--color-text)')}
          onMouseLeave={e => (e.currentTarget.style.color = 'var(--color-text-muted)')}
        >
          → Avançar
        </button>
      )}
    </div>
  )
}
