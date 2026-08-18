import { createClient } from '@/lib/supabase/server'
import { formatBRL, formatBRLCompact, formatDateTime } from '@/lib/utils'
import { STAGE_LABELS, STAGES_ORDERED, normalizeStage, type PipelineStage } from '@/lib/types'
import Link from 'next/link'
import { AlertTriangle, TrendingUp, Building2, FileText, CheckCircle, Clock } from 'lucide-react'

export const dynamic = 'force-dynamic'

export default async function DashboardPage() {
  const supabase = createClient()

  // KPIs
  const { data: kpis } = await supabase
    .from('vw_dashboard_kpis')
    .select('*')
    .single()

  // Funil
  const { data: funil } = await supabase
    .from('vw_funil_estagio')
    .select('*')

  // SLA vencidos (top 5)
  const { data: slaVencidos } = await supabase
    .from('vw_sla_vencido')
    .select('*')
    .limit(5)

  // Empresas com próxima ação hoje
  const hoje = new Date()
  hoje.setHours(0,0,0,0)
  const amanha = new Date(hoje)
  amanha.setDate(amanha.getDate() + 1)

  const { data: followupsHoje } = await supabase
    .from('empresas')
    .select('cnpj_raiz, nome_devedor, estagio, proxima_acao_em, proxima_acao_descricao')
    .gte('proxima_acao_em', hoje.toISOString())
    .lt('proxima_acao_em', amanha.toISOString())
    .eq('ativo', true)
    .eq('excluido', false)
    .order('proxima_acao_em')
    .limit(8)

  const k = kpis || {}
  const totalMotores = (k.motor_a1 || 0) + (k.motor_a2 || 0) + (k.motor_b1 || 0) + (k.motor_b2 || 0)

  // Consolida o funil nas 5 etapas canônicas (a view ainda devolve os valores brutos)
  const funilCanon = STAGES_ORDERED.map(stage => {
    const rows = (funil || []).filter(r => normalizeStage(r.estagio) === stage)
    return {
      stage,
      qtd:   rows.reduce((s, r) => s + (r.qtd_empresas || 0), 0),
      valor: rows.reduce((s, r) => s + (r.valor_total  || 0), 0),
    }
  })
  const maiorEtapa = Math.max(1, ...funilCanon.map(f => f.qtd))

  return (
    <div className="p-6 max-w-7xl mx-auto w-full">

      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-xl font-bold text-text-primary">Dashboard</h1>
          <p className="text-text-muted text-sm">Pipeline PGFN — Seguro Garantia Tributário</p>
        </div>
        <Link href="/oportunidades" className="btn-primary text-xs">
          Ver fila de oportunidades →
        </Link>
      </div>

      {/* KPI Grid */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        <div className="kpi-card">
          <Building2 className="w-4 h-4 text-text-faint mb-1" />
          <p className="kpi-value">{k.total_empresas || 0}</p>
          <p className="kpi-label">Empresas na carteira</p>
          <p className="kpi-sub">{k.em_oportunidade || 0} na fila de oportunidades</p>
        </div>

        <div className="kpi-card">
          <FileText className="w-4 h-4 text-text-faint mb-1" />
          <p className="kpi-value">{k.em_proposta || 0}</p>
          <p className="kpi-label">Em proposta / Sancor</p>
          <p className="kpi-sub">{formatBRLCompact(k.total_divida_carteira)} carteira total</p>
        </div>

        <div className="kpi-card">
          <CheckCircle className="w-4 h-4 text-success mb-1" />
          <p className="kpi-value text-success">{k.convertidos || 0}</p>
          <p className="kpi-label">Convertidos</p>
          <p className="kpi-sub">{formatBRLCompact(k.divida_convertida)} em dívida</p>
        </div>

        <div className="kpi-card">
          <AlertTriangle className="w-4 h-4 text-warning mb-1" />
          <p className="kpi-value text-warning">{k.followups_vencidos || 0}</p>
          <p className="kpi-label">Follow-ups vencidos</p>
          <p className="kpi-sub">SLA &gt; 7 dias</p>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">

        {/* Funil */}
        <div className="lg:col-span-2 card">
          <p className="section-header mb-4">Funil — Empresas por Etapa</p>
          <div className="space-y-2.5">
            {funilCanon.map(({ stage, qtd, valor }) => {
              const pct = (qtd / maiorEtapa) * 100
              return (
                <div key={stage} className="flex items-center gap-3">
                  <span className="text-text-muted text-xs w-24 truncate">{STAGE_LABELS[stage]}</span>
                  <div className="flex-1 bg-bg-secondary rounded-full h-2 overflow-hidden">
                    <div
                      className="h-full bg-vf-red rounded-full transition-all"
                      style={{ width: `${Math.min(pct, 100)}%` }}
                    />
                  </div>
                  <span className="text-text-primary text-xs w-10 text-right tabular-nums">{qtd}</span>
                  <span className="text-text-faint text-xs w-20 text-right tabular-nums">{formatBRLCompact(valor)}</span>
                </div>
              )
            })}
          </div>
        </div>

        {/* Distribuição por Motor */}
        <div className="card">
          <p className="section-header mb-4">Distribuição por Motor</p>
          <div className="space-y-3">
            {[
              { motor: 'A1', label: 'Urgência', count: k.motor_a1 || 0, cls: 'bg-red-500' },
              { motor: 'A2', label: 'Prevenção', count: k.motor_a2 || 0, cls: 'bg-amber-500' },
              { motor: 'B1', label: 'Penhora', count: k.motor_b1 || 0, cls: 'bg-blue-500' },
              { motor: 'B2', label: 'Revisão', count: k.motor_b2 || 0, cls: 'bg-purple-500' },
            ].map(({ motor, label, count, cls }) => {
              const pct = totalMotores ? (count / totalMotores * 100) : 0
              return (
                <div key={motor}>
                  <div className="flex justify-between text-xs mb-1">
                    <span className="text-text-muted"><span className="font-bold text-text-primary">{motor}</span> — {label}</span>
                    <span className="text-text-primary">{count}</span>
                  </div>
                  <div className="bg-bg-secondary rounded-full h-1.5 overflow-hidden">
                    <div className={`h-full ${cls} rounded-full`} style={{ width: `${pct}%` }} />
                  </div>
                </div>
              )
            })}
          </div>

          <div className="mt-4 pt-4 border-t border-border">
            <p className="text-text-faint text-xs">{totalMotores} empresas classificadas</p>
          </div>
        </div>
      </div>

      {/* SLA Vencidos */}
      {slaVencidos && slaVencidos.length > 0 && (
        <div className="card mt-6">
          <div className="flex items-center justify-between mb-4">
            <p className="section-header mb-0 flex items-center gap-2">
              <AlertTriangle className="w-3.5 h-3.5 text-danger" />
              SLA Vencido — Mais de 7 dias sem movimento
            </p>
            <Link href="/pipeline" className="text-xs text-text-muted hover:text-vf-red-light">
              Ver pipeline →
            </Link>
          </div>
          <table className="table-vf">
            <thead>
              <tr>
                <th>Empresa</th>
                <th>Estágio</th>
                <th>Motor</th>
                <th>Responsável</th>
                <th className="text-right">Dias parado</th>
              </tr>
            </thead>
            <tbody>
              {slaVencidos.map(row => (
                <tr key={row.cnpj_raiz}>
                  <td>
                    <Link href={`/empresa/${row.cnpj_raiz}`} className="text-text-primary hover:text-vf-red-light">
                      {row.nome_devedor}
                    </Link>
                  </td>
                  <td className="text-text-muted text-xs">
                    {STAGE_LABELS[normalizeStage(row.estagio)]}
                  </td>
                  <td>
                    {row.motor && (
                      <span className={`motor-${row.motor}`}>{row.motor}</span>
                    )}
                  </td>
                  <td className="text-text-muted">{row.responsavel_nome || '—'}</td>
                  <td className="text-right text-danger font-medium">{row.dias_parado}d</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Follow-ups hoje */}
      {followupsHoje && followupsHoje.length > 0 && (
        <div className="card mt-6">
          <p className="section-header mb-4 flex items-center gap-2">
            <Clock className="w-3.5 h-3.5 text-info" />
            Follow-ups de Hoje
          </p>
          <div className="space-y-2">
            {followupsHoje.map(row => (
              <div key={row.cnpj_raiz} className="flex items-center gap-3 py-2 border-b border-border/50 last:border-0">
                <div className="flex-1">
                  <Link href={`/empresa/${row.cnpj_raiz}`} className="text-text-primary hover:text-vf-red-light text-sm">
                    {row.nome_devedor}
                  </Link>
                  {row.proxima_acao_descricao && (
                    <p className="text-text-muted text-xs">{row.proxima_acao_descricao}</p>
                  )}
                </div>
                <span className="text-text-faint text-xs">
                  {new Date(row.proxima_acao_em).toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' })}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
