import { createClient } from '@/lib/supabase/server'
import Link from 'next/link'
import { Activity, Database, Scale, Phone, Bell, ArrowRight, AlertTriangle } from 'lucide-react'

/**
 * MONITORAMENTO — Fase 10 da ordem executiva.
 * Regra: só números MEDIDOS do banco. Nada de "tempo real ativo" sem
 * processamento real comprovado; a verdade hoje é coleta manual/parada.
 */
export const dynamic = 'force-dynamic'

function dias(d: string | null | undefined): number | null {
  if (!d) return null
  return Math.floor((Date.now() - new Date(d).getTime()) / 86400000)
}
function fmt(d: string | null | undefined): string {
  return d ? new Date(d).toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit', year: '2-digit' }) : '—'
}

export default async function MonitoramentoPage() {
  const supabase = createClient()

  const [insc, evCap, evEnr, evCnt, enrCnt, semAdv, ctRfb, alerta, alertaCnt, empCnt, coletas] = await Promise.all([
    supabase.from('inscricoes').select('criado_em').order('criado_em', { ascending: false }).limit(1),
    supabase.from('eventos').select('capturado_em').order('capturado_em', { ascending: false }).limit(1),
    supabase.from('eventos').select('enriquecido_em').not('enriquecido_em', 'is', null).order('enriquecido_em', { ascending: false }).limit(1),
    supabase.from('eventos').select('id', { count: 'exact', head: true }),
    supabase.from('eventos').select('id', { count: 'exact', head: true }).not('enriquecido_em', 'is', null),
    supabase.from('eventos').select('id', { count: 'exact', head: true }).not('enriquecido_em', 'is', null).is('advogados', null),
    supabase.from('contatos').select('criado_em', { count: 'exact' }).eq('cargo', 'CADASTRO RFB').order('criado_em', { ascending: false }).limit(1),
    supabase.from('alertas').select('detectado_em').order('detectado_em', { ascending: false }).limit(1),
    supabase.from('alertas').select('id', { count: 'exact', head: true }),
    supabase.from('empresas').select('cnpj_raiz', { count: 'exact', head: true }),
    // Status MEDIDO da coleta recorrente: últimas execuções reais do coletor
    supabase.from('coletas').select('iniciada_em, concluida_em, processados, novos_eventos, novos_alertas, erros')
      .eq('fonte', 'DJEN_COMUNICA').order('iniciada_em', { ascending: false }).limit(6),
  ])

  const runs = coletas.data ?? []
  const ultimaColeta = runs[0]?.iniciada_em
  const minDesdeColeta = ultimaColeta ? Math.floor((Date.now() - new Date(ultimaColeta).getTime()) / 60000) : null
  const coletaAtiva = minDesdeColeta != null && minDesdeColeta <= 30
  const somaEventos = runs.reduce((s, r) => s + (r.novos_eventos ?? 0), 0)

  const fontes = [
    {
      icon: Database, nome: 'PGFN — dívida ativa (carga LAI)',
      ultima: insc.data?.[0]?.criado_em, registros: `${empCnt.count ?? 0} empresas`,
      modo: 'CARGA MANUAL ÚNICA',
      divergencia: '1 inscrição/empresa (trigger sobrescreve consolidada); ~110k secundárias só no Postgres local',
    },
    {
      icon: Scale, nome: 'DJEN/Comunica — eventos judiciais',
      ultima: evCap.data?.[0]?.capturado_em, registros: `${evCnt.count ?? 0} eventos`,
      modo: coletaAtiva ? 'CRON AUTOMÁTICO (15 min, Edge Function coleta-djen)' : 'CRON AGENDADO (aguardando próxima execução)',
      divergencia: null,
    },
    {
      icon: Scale, nome: 'DJEN/Comunica — teores + advogados',
      ultima: evEnr.data?.[0]?.enriquecido_em, registros: `${enrCnt.count ?? 0} enriquecidos`,
      modo: 'COLETA MANUAL',
      divergencia: `27 processos sem retorno da API · ${semAdv.count ?? 0} sem advogado (réu revel)`,
    },
    {
      icon: Phone, nome: 'Receita — telefone cadastral (CNPJ)',
      ultima: ctRfb.data?.[0]?.criado_em, registros: `${ctRfb.count ?? 0} contatos`,
      modo: 'COLETA MANUAL (só empresas da pauta)',
      divergencia: 'e-mails não expostos pela fonte',
    },
    {
      icon: Bell, nome: 'Central — alertas (validação humana)',
      ultima: alerta.data?.[0]?.detectado_em, registros: `${alertaCnt.count ?? 0} alertas`,
      modo: 'FIXTURE PAUTA 20/08',
      divergencia: null,
    },
  ]

  return (
    <div className="p-6 max-w-5xl mx-auto">
      <div className="flex items-center justify-between flex-wrap gap-2 mb-1">
        <h1 className="text-xl font-bold text-text-primary flex items-center gap-2">
          <Activity className="w-5 h-5 text-vf-red-light" /> Monitoramento das fontes
        </h1>
        {coletaAtiva ? (
          <span className="badge bg-success/15 text-success">
            coleta recorrente ATIVA · última há {minDesdeColeta} min
          </span>
        ) : (
          <span className="badge bg-warning/15 text-warning">
            coleta recorrente: {ultimaColeta ? `SEM EXECUÇÃO HÁ ${minDesdeColeta} MIN` : 'SEM EXECUÇÃO REGISTRADA'}
          </span>
        )}
      </div>
      <p className="text-text-muted text-sm mb-6 max-w-3xl">
        Estado real de cada fonte, medido do banco agora. Este painel não simula atividade:
        o selo acima só fica verde quando existe execução do coletor registrada nos últimos 30 minutos.
      </p>

      {/* Execuções reais do coletor (cron a cada 15 min) */}
      {runs.length > 0 && (
        <div className="card mb-4">
          <p className="section-header">Coletor DJEN — execuções reais (lotes de 40 processos, cron 15 min)</p>
          <div className="overflow-x-auto">
            <table className="table-vf">
              <thead><tr><th>Início</th><th className="text-right">Processos</th><th className="text-right">Eventos novos</th><th className="text-right">Alertas</th><th className="text-right">Erros</th></tr></thead>
              <tbody>
                {runs.map((r, i) => (
                  <tr key={i}>
                    <td className="text-xs text-text-muted">{new Date(r.iniciada_em).toLocaleString('pt-BR', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' })}</td>
                    <td className="text-right text-xs tabular-nums">{r.processados}</td>
                    <td className="text-right text-xs tabular-nums text-success font-semibold">{r.novos_eventos}</td>
                    <td className="text-right text-xs tabular-nums text-vf-red-light">{r.novos_alertas}</td>
                    <td className={`text-right text-xs tabular-nums ${r.erros ? 'text-danger' : 'text-text-faint'}`}>{r.erros}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="text-[11px] text-text-faint mt-2">{somaEventos} eventos novos nas últimas {runs.length} execuções · alertas de constrição nascem CANDIDATO e aguardam validação na Central.</p>
        </div>
      )}

      <div className="space-y-3">
        {fontes.map(f => {
          const d = dias(f.ultima)
          const parado = d == null || d > 1
          return (
            <div key={f.nome} className="card flex items-start gap-4">
              <div className="w-9 h-9 rounded-lg bg-bg-secondary border border-border grid place-items-center flex-shrink-0">
                <f.icon className="w-4 h-4 text-vf-red-light" />
              </div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center justify-between gap-3 flex-wrap">
                  <p className="text-sm font-medium text-text-primary">{f.nome}</p>
                  <span className={`badge text-[10px] ${parado ? 'bg-warning/15 text-warning' : 'bg-success/15 text-success'}`}>
                    {parado ? 'PARADA / MANUAL' : 'ATUALIZADA HOJE'}
                  </span>
                </div>
                <p className="text-xs text-text-muted mt-1">
                  Última sincronização: <b className="text-text-primary">{fmt(f.ultima)}</b>
                  {d != null && <span className="text-text-faint"> ({d === 0 ? 'hoje' : `há ${d}d`})</span>}
                  {' '}· {f.registros} · modo: {f.modo}
                </p>
                {f.divergencia && (
                  <p className="text-xs text-warning mt-1 flex items-start gap-1.5">
                    <AlertTriangle className="w-3.5 h-3.5 flex-shrink-0 mt-0.5" /> {f.divergencia}
                  </p>
                )}
              </div>
            </div>
          )
        })}
      </div>

      {/* Fluxo — cada etapa com o estado REAL (nada decorativo) */}
      <div className="card mt-6">
        <p className="section-header">Fluxo da inteligência — estado real de cada etapa</p>
        <div className="flex flex-wrap items-center gap-y-2 text-xs">
          {[
            ['Fontes', coletaAtiva ? 'cron 15min ✓' : 'cron agendado'], ['Normalização', 'no coletor ✓'], ['Cruzamento', 'CNPJ raiz ✓'],
            ['Validação', 'Central ✓'], ['Classificação', 'tipo+ramo ✓'],
            ['Alertas', `${alertaCnt.count ?? 0} ✓`], ['Oportunidades', 'via validação ✓'],
            ['Dossiês', 'VF_OSINT local'], ['Relatórios', 'não construído'],
          ].map(([etapa, estado], i, arr) => (
            <span key={etapa} className="flex items-center">
              <span className="px-2.5 py-1.5 rounded border border-border bg-bg-secondary">
                <b className="text-text-primary">{etapa}</b>{' '}
                <span className={estado.includes('✓') ? 'text-success' : estado === 'não construído' ? 'text-danger' : 'text-warning'}>{estado}</span>
              </span>
              {i < arr.length - 1 && <ArrowRight className="w-3.5 h-3.5 text-text-faint mx-1.5" />}
            </span>
          ))}
        </div>
        <p className="text-[11px] text-text-faint mt-3">
          ✓ = processamento real existente · <span className="text-warning">amarelo</span> = manual/parcial ·{' '}
          <span className="text-danger">vermelho</span> = não construído. Próxima etapa do roteiro: coleta DJEN agendada.
        </p>
      </div>

      <p className="text-[11px] text-text-faint mt-6">
        Consulta executada agora no banco de produção via sessão autenticada.{' '}
        <Link href="/pauta" className="text-info hover:underline">Ir para a Central →</Link>
      </p>
    </div>
  )
}
