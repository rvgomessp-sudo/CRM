// app/(painel)/dashboard/page.tsx
import { createClient } from '@/lib/supabase/server'
import { formatBRL } from '@/lib/utils'
import { ESTAGIO_LABELS, EstagioPipeline, ESTAGIO_ORDEM } from '@/lib/types'

export const dynamic = 'force-dynamic'

export default async function DashboardPage() {
  const supabase = await createClient()

  // KPIs gerais
  const { data: empresas } = await supabase
    .from('empresas')
    .select('estagio, prioridade, valor_total_brl, seguradora')
    .eq('excluido', false)

  // Follow-ups vencidos
  const { data: followups } = await supabase
    .from('vw_followups_vencidos')
    .select('*')
    .limit(10)

  // SLA alertas
  const { data: slaAlertas } = await supabase
    .from('vw_sla_alertas')
    .select('*')
    .limit(5)

  const total = empresas?.length || 0
  const valorCarteira = empresas?.reduce((acc, e) => acc + (e.valor_total_brl || 0), 0) || 0
  const emPipeline = empresas?.filter(e => !['base_pgfn', 'receita_realizada'].includes(e.estagio)).length || 0
  const fechados = empresas?.filter(e => e.estagio === 'receita_realizada').length || 0
  const alta = empresas?.filter(e => e.prioridade === 'ALTA').length || 0

  // Funil por estágio
  const funnelMap = new Map<EstagioPipeline, number>()
  empresas?.forEach(e => {
    funnelMap.set(e.estagio, (funnelMap.get(e.estagio) || 0) + 1)
  })

  return (
    <div>
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 20, fontWeight: 700, marginBottom: 4 }}>Dashboard</h1>
        <p style={{ color: 'var(--color-text-muted)', fontSize: 13 }}>
          Visão geral da operação PGFN — Esteira Sancor
        </p>
      </div>

      {/* KPIs */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: 12, marginBottom: 24 }}>
        {[
          { label: 'Total Empresas', value: total, sub: `${alta} alta prioridade` },
          { label: 'Carteira Total', value: formatBRL(valorCarteira), sub: 'soma F2' },
          { label: 'Em Pipeline', value: emPipeline, sub: 'etapas 2-9' },
          { label: 'Receita Realizada', value: fechados, sub: 'operações' },
          { label: 'Follow-ups Vencidos', value: followups?.length || 0, sub: 'requer ação', danger: true },
        ].map(kpi => (
          <div key={kpi.label} className="kpi-card" style={kpi.danger && (followups?.length || 0) > 0 ? { borderColor: 'rgba(239,68,68,0.3)' } : {}}>
            <div className="kpi-label">{kpi.label}</div>
            <div className="kpi-value" style={kpi.danger && (followups?.length || 0) > 0 ? { color: '#f87171' } : {}}>
              {kpi.value}
            </div>
            <div className="kpi-sub">{kpi.sub}</div>
          </div>
        ))}
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20 }}>
        {/* Funil */}
        <div className="card">
          <h2 style={{ fontSize: 14, fontWeight: 600, marginBottom: 16 }}>Funil do Pipeline</h2>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            {ESTAGIO_ORDEM.map((estagio, idx) => {
              const qtd = funnelMap.get(estagio) || 0
              const pct = total > 0 ? (qtd / total) * 100 : 0
              const colors = ['#374151','#1e3a5f','#1e40af','#4c1d95','#7c3aed','#b45309','#065f46','#14532d','#166534','#15803d']
              return (
                <div key={estagio} style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                  <div style={{ width: 120, fontSize: 11, color: 'var(--color-text-muted)', flexShrink: 0 }}>
                    {ESTAGIO_LABELS[estagio]}
                  </div>
                  <div style={{ flex: 1, background: 'var(--color-surface-2)', borderRadius: 3, height: 20, overflow: 'hidden' }}>
                    <div style={{
                      width: `${Math.max(pct, qtd > 0 ? 3 : 0)}%`,
                      height: '100%',
                      background: colors[idx],
                      borderRadius: 3,
                      display: 'flex', alignItems: 'center',
                      paddingLeft: 6,
                      fontSize: 10,
                      color: 'rgba(255,255,255,0.8)',
                      transition: 'width 0.3s',
                    }}>
                      {qtd > 0 && qtd}
                    </div>
                  </div>
                  <div style={{ width: 28, textAlign: 'right', fontSize: 12, fontWeight: 600 }}>{qtd}</div>
                </div>
              )
            })}
          </div>
        </div>

        {/* Follow-ups vencidos */}
        <div className="card">
          <h2 style={{ fontSize: 14, fontWeight: 600, marginBottom: 16, display: 'flex', alignItems: 'center', gap: 8 }}>
            Follow-ups Vencidos
            {(followups?.length || 0) > 0 && (
              <span style={{
                background: 'rgba(239,68,68,0.15)',
                color: '#f87171',
                fontSize: 11,
                fontWeight: 700,
                padding: '2px 8px',
                borderRadius: 12,
              }}>{followups?.length}</span>
            )}
          </h2>
          {followups && followups.length > 0 ? (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              {followups.slice(0, 8).map((f: Record<string, string | number>) => (
                <div key={f.interacao_id} style={{
                  display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                  padding: '8px 10px',
                  background: 'var(--color-surface-2)',
                  borderRadius: 6,
                  fontSize: 12,
                }}>
                  <div>
                    <div style={{ fontWeight: 500 }}>{f.nome_devedor}</div>
                    <div style={{ color: 'var(--color-text-muted)', fontSize: 11 }}>{f.proxima_acao}</div>
                  </div>
                  <div style={{
                    background: Number(f.dias_atraso) > 7 ? 'rgba(239,68,68,0.15)' : 'rgba(245,158,11,0.15)',
                    color: Number(f.dias_atraso) > 7 ? '#f87171' : '#fbbf24',
                    padding: '2px 8px', borderRadius: 10, fontSize: 11, fontWeight: 600,
                  }}>
                    +{f.dias_atraso}d
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <p style={{ color: 'var(--color-text-muted)', fontSize: 13 }}>Nenhum follow-up vencido.</p>
          )}
        </div>
      </div>

      {/* SLA Alertas */}
      {slaAlertas && slaAlertas.length > 0 && (
        <div className="card" style={{ marginTop: 20 }}>
          <h2 style={{ fontSize: 14, fontWeight: 600, marginBottom: 12 }}>⚠ Casos Parados {'>'} 7 dias</h2>
          <table>
            <thead>
              <tr>
                <th>Empresa</th>
                <th>Estágio</th>
                <th>Dias Parado</th>
              </tr>
            </thead>
            <tbody>
              {slaAlertas.map((a: Record<string, string | number>) => (
                <tr key={a.cnpj_raiz}>
                  <td style={{ fontWeight: 500 }}>{a.nome_devedor}</td>
                  <td style={{ color: 'var(--color-text-muted)' }}>{ESTAGIO_LABELS[a.estagio as EstagioPipeline]}</td>
                  <td>
                    <span style={{
                      background: 'rgba(239,68,68,0.1)',
                      color: '#f87171',
                      padding: '2px 8px', borderRadius: 10, fontSize: 11, fontWeight: 600,
                    }}>{a.dias_parado} dias</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
