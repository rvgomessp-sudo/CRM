import { createClient } from '@/lib/supabase/server'
import { formatBRLCompact } from '@/lib/utils'
import {
  STAGE_LABELS, STAGES_ORDERED, normalizeStage,
  EVENTO_JUDICIAL_LABELS, ZONA_LABELS,
} from '@/lib/types'
import Link from 'next/link'
import {
  AlertTriangle, Building2, FileText, CheckCircle, Clock,
  ShieldAlert, Zap, ArrowRight, Activity,
} from 'lucide-react'

export const dynamic = 'force-dynamic'

function diasDe(d: string | null | undefined): number | null {
  if (!d) return null
  return Math.floor((Date.now() - new Date(d).getTime()) / 86400000)
}

const ZONA_META: Record<string, { rank: number; dot: string; bar: string; text: string }> = {
  SUFOCO:   { rank: 0, dot: 'bg-danger',  bar: 'bg-danger',  text: 'text-danger'  },
  VERMELHA: { rank: 1, dot: 'bg-rose-deep', bar: 'bg-rose-deep', text: 'text-rose-light' },
  AMARELA:  { rank: 2, dot: 'bg-warning', bar: 'bg-warning', text: 'text-warning' },
  VERDE:    { rank: 3, dot: 'bg-success', bar: 'bg-success', text: 'text-success' },
}

export default async function DashboardPage() {
  const supabase = createClient()

  const seteDias = new Date(Date.now() - 7 * 86400000).toISOString().slice(0, 10)

  const [
    { data: kpis },
    { data: funil },
    { data: slaVencidos },
    { data: radar },
    { data: zonaRows },
    { data: pautaRows },
    { data: eventosRecentes },
    { data: provColeta },
    { data: provEnriquecimento },
    { data: provInscricoes },
  ] = await Promise.all([
    supabase.from('vw_dashboard_kpis').select('*').single(),
    supabase.from('vw_funil_estagio').select('*'),
    supabase.from('vw_sla_vencido').select('*').limit(5),
    // Radar de sufoco: as oportunidades mais quentes com constrição ativa
    supabase.from('vw_fila_oportunidades')
      .select('cnpj_raiz, nome_devedor, uf_devedor, score, zona_risco, motor, tributo_principal, evento_judicial_tipo, evento_judicial_em, valor_total_devida')
      .order('score', { ascending: false, nullsFirst: false })
      .limit(8),
    // Triagem por zona de risco
    supabase.from('vw_fila_oportunidades').select('zona_risco, valor_total_devida, evento_judicial_tipo'),
    // Pauta do dia (a linha de trabalho)
    supabase.from('pauta').select('data, concluido').order('data', { ascending: false }),
    // O que mudou: eventos judiciais dos últimos 7 dias
    supabase.from('eventos').select('tipo, ocorrido_em').eq('fonte', 'JUDICIAL').gte('ocorrido_em', seteDias),
    // Proveniência — o carimbo de frescor de cada camada de dado
    supabase.from('eventos').select('capturado_em').order('capturado_em', { ascending: false }).limit(1),
    supabase.from('eventos').select('enriquecido_em').not('enriquecido_em', 'is', null).order('enriquecido_em', { ascending: false }).limit(1),
    supabase.from('inscricoes').select('criado_em').order('criado_em', { ascending: false }).limit(1),
  ])

  // Pauta mais recente
  const pautaData = pautaRows?.[0]?.data ?? null
  const pautaDoDia = (pautaRows || []).filter(p => p.data === pautaData)
  const pautaFeitos = pautaDoDia.filter(p => p.concluido).length

  // O que mudou nos últimos 7 dias, por tipo
  const mudouMap = new Map<string, number>()
  for (const e of (eventosRecentes || [])) mudouMap.set(e.tipo, (mudouMap.get(e.tipo) || 0) + 1)
  const mudou = Array.from(mudouMap.entries()).sort((a, b) => b[1] - a[1])
  const totalMudou = (eventosRecentes || []).length

  const fmtDia = (d: string | null | undefined) =>
    d ? new Date(d).toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit' }) : '—'

  const k = kpis || {}
  const totalMotores = (k.motor_a1 || 0) + (k.motor_a2 || 0) + (k.motor_b1 || 0) + (k.motor_b2 || 0)
  const emNegociacao = (k.em_abordagem || 0) + (k.em_proposta || 0) + (k.em_seguradora || 0)

  // Consolida o funil nas 5 etapas canônicas
  const funilCanon = STAGES_ORDERED.map(stage => {
    const rows = (funil || []).filter(r => normalizeStage(r.estagio) === stage)
    return {
      stage,
      qtd:   rows.reduce((s, r) => s + (r.qtd_empresas || 0), 0),
      valor: rows.reduce((s, r) => s + (r.valor_total  || 0), 0),
    }
  })
  const maiorEtapa = Math.max(1, ...funilCanon.map(f => f.qtd))

  // Triagem por zona (a partir da fila)
  const CONSTRICAO = new Set(['SISBAJUD', 'PENHORA', 'RENAJUD'])
  const zonaMap = new Map<string, { n: number; divida: number; constricao: number }>()
  for (const r of (zonaRows || [])) {
    const z = r.zona_risco || 'SEM_ZONA'
    const cur = zonaMap.get(z) || { n: 0, divida: 0, constricao: 0 }
    cur.n += 1
    cur.divida += Number(r.valor_total_devida) || 0
    if (CONSTRICAO.has(r.evento_judicial_tipo)) cur.constricao += 1
    zonaMap.set(z, cur)
  }
  const zonas = Array.from(zonaMap.entries())
    .map(([zona, v]) => ({ zona, ...v }))
    .sort((a, b) => (ZONA_META[a.zona]?.rank ?? 9) - (ZONA_META[b.zona]?.rank ?? 9))
  const totalFila = zonas.reduce((s, z) => s + z.n, 0) || 1
  const sufocoAtivo = zonaMap.get('SUFOCO')?.constricao ?? 0
  const dividaSufoco = zonaMap.get('SUFOCO')?.divida ?? 0

  return (
    <div className="p-6 max-w-7xl mx-auto w-full">

      {/* Header */}
      <div className="flex items-center justify-between mb-6 gap-3 flex-wrap">
        <div>
          <h1 className="text-xl font-bold text-text-primary">Cockpit</h1>
          <p className="text-text-muted text-sm">
            Garantia sem Barreiras · eventos coletados até <span className="text-text-primary">{fmtDia(provColeta?.[0]?.capturado_em)}</span> · teores DJEN de <span className="text-text-primary">{fmtDia(provEnriquecimento?.[0]?.enriquecido_em)}</span>
          </p>
        </div>
        <div className="flex gap-2">
          <Link href="/pauta" className="btn-primary text-xs">
            Pauta do dia <ArrowRight className="w-3.5 h-3.5" />
          </Link>
          <Link href="/oportunidades" className="btn-secondary text-xs">Fila completa</Link>
        </div>
      </div>

      {/* KPI Grid — cada número aponta para uma decisão */}
      <div className="grid grid-cols-2 lg:grid-cols-5 gap-4 mb-6">
        {/* Pauta do dia — a linha de trabalho */}
        <Link href="/pauta"
          className="kpi-card border-vf-red/40 bg-gradient-to-br from-vf-red/10 to-transparent hover:border-vf-red/60 transition-colors">
          <Clock className="w-4 h-4 text-vf-red-light mb-1" />
          <p className="kpi-value text-vf-red-light">{pautaFeitos}<span className="text-text-faint text-lg">/{pautaDoDia.length}</span></p>
          <p className="kpi-label">Pauta {pautaData ? `de ${fmtDia(pautaData)}` : ''}</p>
          <p className="kpi-sub">abordagens concluídas</p>
        </Link>

        <div className="kpi-card">
          <Building2 className="w-4 h-4 text-text-faint mb-1" />
          <p className="kpi-value">{k.total_empresas || 0}</p>
          <p className="kpi-label">Empresas na carteira</p>
          <p className="kpi-sub">{k.em_oportunidade || 0} na fila · {formatBRLCompact(k.total_divida_carteira)} em dívida</p>
        </div>

        <div className="kpi-card">
          <FileText className="w-4 h-4 text-text-faint mb-1" />
          <p className="kpi-value">{emNegociacao}</p>
          <p className="kpi-label">Em negociação ativa</p>
          <p className="kpi-sub">{formatBRLCompact(k.divida_em_jogo)} em jogo</p>
        </div>

        <div className="kpi-card">
          <CheckCircle className="w-4 h-4 text-success mb-1" />
          <p className="kpi-value text-success">{k.convertidos || 0}</p>
          <p className="kpi-label">Convertidos</p>
          <p className="kpi-sub">{formatBRLCompact(k.divida_convertida)} em dívida garantida</p>
        </div>

        {/* Sufoco ativo — o KPI de alerta: bloqueio de conta acontecendo agora */}
        <Link href="/oportunidades?zona=SUFOCO"
          className="kpi-card border-danger/40 bg-gradient-to-br from-danger/10 to-transparent hover:border-danger/60 transition-colors">
          <ShieldAlert className="w-4 h-4 text-danger mb-1" />
          <p className="kpi-value text-danger">{sufocoAtivo}</p>
          <p className="kpi-label">Em sufoco — bloqueio ativo</p>
          <p className="kpi-sub">{formatBRLCompact(dividaSufoco)} sob constrição SISBAJUD</p>
        </Link>
      </div>

      {/* Radar de sufoco + Triagem por zona */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-6">

        {/* Radar de sufoco — a superfície de decisão principal */}
        <div className="lg:col-span-2 card">
          <div className="flex items-center justify-between mb-4">
            <p className="section-header mb-0 flex items-center gap-2">
              <Zap className="w-3.5 h-3.5 text-danger" /> Radar de sufoco — abordar primeiro
            </p>
            <Link href="/oportunidades" className="text-xs text-text-muted hover:text-rose-light">Ver fila →</Link>
          </div>

          {(!radar || radar.length === 0) ? (
            <p className="text-text-faint text-xs">Nenhuma oportunidade priorizada.</p>
          ) : (
            <div className="space-y-1.5">
              {radar.map((r) => {
                const dias = diasDe(r.evento_judicial_em)
                const zm = ZONA_META[r.zona_risco] ?? ZONA_META.AMARELA
                const ev = r.evento_judicial_tipo
                  ? (EVENTO_JUDICIAL_LABELS[r.evento_judicial_tipo] ?? r.evento_judicial_tipo)
                  : null
                const score = r.score != null ? Math.round(Number(r.score)) : null
                return (
                  <Link key={r.cnpj_raiz} href={`/empresa/${r.cnpj_raiz}`}
                    className="flex items-center gap-3 px-2 py-2 rounded-lg hover:bg-bg-hover transition-colors group">
                    {/* Score */}
                    <div className="flex-shrink-0 w-11 text-center">
                      <span className="text-base font-bold text-rose-light tabular-nums leading-none">{score ?? '—'}</span>
                      <span className="block text-[8px] uppercase tracking-widest text-text-faint">score</span>
                    </div>
                    {/* Nome + evento */}
                    <div className="min-w-0 flex-1">
                      <p className="text-sm font-medium text-text-primary truncate group-hover:text-rose-light transition-colors">
                        {r.nome_devedor}
                      </p>
                      <div className="flex items-center gap-1.5 mt-0.5 flex-wrap">
                        <span className={`inline-flex items-center gap-1 text-[10px] font-semibold ${zm.text}`}>
                          <span className={`w-1.5 h-1.5 rounded-full ${zm.dot}`} />
                          {ZONA_LABELS[r.zona_risco] ?? r.zona_risco}
                        </span>
                        {ev && (
                          <span className="text-[10px] text-text-muted">
                            · {ev}{dias != null && <> há {dias === 0 ? '<1d' : `${dias}d`}</>}
                          </span>
                        )}
                        <span className="text-[10px] text-text-faint">· {r.uf_devedor}</span>
                      </div>
                    </div>
                    {/* Dívida + motor/tributo (seguradora não é mais direcionada automaticamente) */}
                    <div className="flex-shrink-0 text-right">
                      <p className="text-sm font-semibold text-text-primary tabular-nums">{formatBRLCompact(r.valor_total_devida)}</p>
                      <p className="text-[10px] text-text-faint">{r.motor}{r.tributo_principal ? ` · ${r.tributo_principal}` : ''}</p>
                    </div>
                    <ArrowRight className="w-3.5 h-3.5 text-text-faint group-hover:text-rose-light transition-colors flex-shrink-0" />
                  </Link>
                )
              })}
            </div>
          )}
        </div>

        {/* Triagem por zona de risco */}
        <div className="card">
          <p className="section-header flex items-center gap-2">
            <Activity className="w-3.5 h-3.5 text-rose-light" /> Triagem por zona
          </p>
          <div className="space-y-4">
            {zonas.map(({ zona, n, divida, constricao }) => {
              const zm = ZONA_META[zona] ?? { dot: 'bg-text-faint', bar: 'bg-text-faint', text: 'text-text-muted' }
              const pct = (n / totalFila) * 100
              return (
                <div key={zona}>
                  <div className="flex items-center justify-between mb-1.5">
                    <span className={`inline-flex items-center gap-1.5 text-xs font-semibold ${zm.text}`}>
                      <span className={`w-2 h-2 rounded-full ${zm.dot}`} />
                      {ZONA_LABELS[zona] ?? zona}
                    </span>
                    <span className="text-text-primary text-xs tabular-nums">{n}</span>
                  </div>
                  <div className="bg-bg-secondary rounded-full h-1.5 overflow-hidden">
                    <div className={`h-full ${zm.bar} rounded-full`} style={{ width: `${pct}%` }} />
                  </div>
                  <div className="flex items-center justify-between mt-1">
                    <span className="text-text-faint text-[10px]">{formatBRLCompact(divida)} em dívida</span>
                    {constricao > 0 && (
                      <span className="text-danger text-[10px] font-medium">{constricao} com bloqueio</span>
                    )}
                  </div>
                </div>
              )
            })}
          </div>
          <p className="text-text-faint text-[10px] mt-4 pt-3 border-t border-border leading-relaxed">
            Zona = leitura do risco processual. <span className="text-danger">Sufoco</span> tem constrição em curso — o seguro garantia substitui o bloqueio e libera o caixa.
          </p>

          {/* O que mudou — a pergunta nº 1 de um monitor */}
          <div className="mt-5 pt-4 border-t border-border">
            <p className="section-header flex items-center gap-2">
              <Zap className="w-3.5 h-3.5 text-warning" /> O que mudou · 7 dias
            </p>
            {totalMudou === 0 ? (
              <p className="text-text-faint text-xs">Nenhum evento novo no período — coleta recorrente ainda não ligada.</p>
            ) : (
              <div className="space-y-1.5">
                {mudou.slice(0, 5).map(([tipo, n]) => (
                  <div key={tipo} className="flex items-center justify-between text-xs">
                    <span className="text-text-muted">{EVENTO_JUDICIAL_LABELS[tipo] ?? tipo}</span>
                    <span className="text-text-primary font-semibold tabular-nums">{n}</span>
                  </div>
                ))}
                <p className="text-text-faint text-[10px] pt-1">{totalMudou} eventos judiciais novos no período</p>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Funil + Motor */}
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
                    <div className="h-full bg-vf-red rounded-full transition-all" style={{ width: `${Math.min(pct, 100)}%` }} />
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
              { motor: 'A1', label: 'Urgência', count: k.motor_a1 || 0, cls: 'bg-motor-A1' },
              { motor: 'A2', label: 'Prevenção', count: k.motor_a2 || 0, cls: 'bg-motor-A2' },
              { motor: 'B1', label: 'Penhora', count: k.motor_b1 || 0, cls: 'bg-motor-B1' },
              { motor: 'B2', label: 'Revisão', count: k.motor_b2 || 0, cls: 'bg-motor-B2' },
            ].map(({ motor, label, count, cls }) => {
              const pct = totalMotores ? (count / totalMotores * 100) : 0
              return (
                <div key={motor}>
                  <div className="flex justify-between text-xs mb-1">
                    <span className="text-text-muted"><span className="font-bold text-text-primary">{motor}</span> — {label}</span>
                    <span className="text-text-primary tabular-nums">{count}</span>
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
              SLA vencido — mais de 7 dias sem movimento
            </p>
            <Link href="/pipeline" className="text-xs text-text-muted hover:text-rose-light">Ver pipeline →</Link>
          </div>
          <table className="table-vf">
            <thead>
              <tr>
                <th>Empresa</th><th>Estágio</th><th>Motor</th><th>Responsável</th>
                <th className="text-right">Dias parado</th>
              </tr>
            </thead>
            <tbody>
              {slaVencidos.map(row => (
                <tr key={row.cnpj_raiz}>
                  <td>
                    <Link href={`/empresa/${row.cnpj_raiz}`} className="text-text-primary hover:text-rose-light">
                      {row.nome_devedor}
                    </Link>
                  </td>
                  <td className="text-text-muted text-xs">{STAGE_LABELS[normalizeStage(row.estagio)]}</td>
                  <td>{row.motor && <span className={`motor-${row.motor}`}>{row.motor}</span>}</td>
                  <td className="text-text-muted">{row.responsavel_nome || '—'}</td>
                  <td className="text-right text-danger font-medium tabular-nums">{row.dias_parado}d</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Proveniência — todo dado tem carimbo; sem data visível, é boato */}
      <div className="mt-6 pt-4 border-t border-border flex items-center gap-x-6 gap-y-1 flex-wrap text-[11px] text-text-faint">
        <span className="uppercase tracking-widest font-semibold text-text-muted">Saúde das fontes</span>
        <span>Eventos judiciais (DJEN): coletados em <b className="text-text-muted">{fmtDia(provColeta?.[0]?.capturado_em)}</b></span>
        <span>Teores + advogados: enriquecidos em <b className="text-text-muted">{fmtDia(provEnriquecimento?.[0]?.enriquecido_em)}</b></span>
        <span>Dívida PGFN: carga de <b className="text-text-muted">{fmtDia(provInscricoes?.[0]?.criado_em)}</b> <span className="text-warning">(parcial — consolidação pendente)</span></span>
        <span className="text-text-faint">Coleta recorrente: <span className="text-warning">não ligada</span> — próxima fase</span>
      </div>
    </div>
  )
}
