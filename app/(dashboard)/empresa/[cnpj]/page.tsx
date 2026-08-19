'use client'

import { useEffect, useState, type ReactNode } from 'react'
import { useParams } from 'next/navigation'
import Link from 'next/link'
import { createClient } from '@/lib/supabase/client'
import {
  formatBRL, formatBRLCompact, formatDate, formatDateTime,
  formatCnpj, cn
} from '@/lib/utils'
import {
  STAGE_LABELS, STAGES_ORDERED, MOTOR_COLORS, MOTOR_BADGE_LABEL, normalizeStage,
  EVENTO_JUDICIAL_LABELS, EVENTO_ALERTA_MAXIMO,
  ZONA_LABELS, ZONA_COLORS, ZONA_DESCRICAO,
  DESFECHO_LABELS, DESFECHO_COLORS, TIPO_FECHAMENTO_LABELS,
  type Empresa, type Inscricao, type Interacao, type ConsultaSeguradora,
  type Proposta, type PipelineStage, type MotorTipo, type CanalInteracao
} from '@/lib/types'
import { DesfechoModal, type DesfechoPayload } from '@/components/DesfechoModal'
import {
  ArrowLeft, Building2, FileText, MessageSquare, Calculator, ShieldAlert,
  AlertTriangle, CheckCircle, Clock, Plus, ChevronDown, Phone, Mail, Trash2,
  User, Users, Gavel, Landmark, Hammer, Target,
  Linkedin, Zap, ArrowRight, Shield, Sparkles,
} from 'lucide-react'

type Tab = 'painel' | 'inscricoes' | 'contatos' | 'interacoes' | 'proposta'

interface AdvogadoDJEN { nome: string; oab?: string | null; uf?: string | null }

interface EventoJudicial {
  id: string
  tipo: string
  numero_processo: string | null
  ocorrido_em: string | null
  payload: { polo?: string; tribunal?: string; ramo?: string } | null
  texto: string | null
  advogados: AdvogadoDJEN[] | null
  link_publicacao: string | null
}

// Fase 6 — resumo do alerta da Central (a validação vem antes da abordagem)
interface AlertaResumo {
  id: string
  titulo: string
  gravidade: string
  estado: string
  evidencia_condicao: string
  papel_processual: string
  pendencias: string[] | null
  numero_processo: string | null
  detectado_em: string
}

function diasDe(d: string | null | undefined): number | null {
  if (!d) return null
  return Math.floor((Date.now() - new Date(d).getTime()) / 86400000)
}

export default function EmpresaPage() {
  const params = useParams()
  const cnpj = params.cnpj as string
  const supabase = createClient()

  const [empresa, setEmpresa] = useState<Empresa | null>(null)
  const [inscricoes, setInscricoes] = useState<Inscricao[]>([])
  const [interacoes, setInteracoes] = useState<Interacao[]>([])
  const [consultas, setConsultas] = useState<ConsultaSeguradora[]>([])
  const [propostas, setPropostas] = useState<Proposta[]>([])
  const [contatos, setContatos] = useState<any[]>([])
  const [eventos, setEventos] = useState<EventoJudicial[]>([])
  const [alertas, setAlertas] = useState<AlertaResumo[]>([])
  const [score, setScore] = useState<number | null>(null)
  const [alvoMarinheiro, setAlvoMarinheiro] = useState(false)
  const [dossieLoading, setDossieLoading] = useState(false)
  const [dossieMsg, setDossieMsg] = useState<string | null>(null)
  const [showContatoForm, setShowContatoForm] = useState(false)
  const [novoContato, setNovoContato] = useState({ nome: '', cargo: '', telefone: '', email: '' })
  const [loading, setLoading] = useState(true)
  const [tab, setTab] = useState<Tab>('painel')
  const [saving, setSaving] = useState(false)
  const [desfechoOpen, setDesfechoOpen] = useState(false)
  const [showInteracaoForm, setShowInteracaoForm] = useState(false)
  const [novaInteracao, setNovaInteracao] = useState({
    canal: 'TELEFONE' as CanalInteracao,
    resumo: '', proxima_acao: '', proxima_acao_em: ''
  })

  useEffect(() => { fetchAll() }, [cnpj]) // eslint-disable-line react-hooks/exhaustive-deps

  async function fetchAll() {
    setLoading(true)
    const [emp, ins, int, con, prop, cont, evt, fil, alr] = await Promise.all([
      supabase.from('empresas').select('*').eq('cnpj_raiz', cnpj).single(),
      supabase.from('inscricoes').select('*').eq('cnpj_raiz', cnpj).order('valor_numerico', { ascending: false }),
      supabase.from('interacoes').select('*').eq('cnpj_raiz', cnpj).order('criado_em', { ascending: false }),
      supabase.from('consultas_seguradora').select('*').eq('cnpj_raiz', cnpj).order('data_consulta', { ascending: false }),
      supabase.from('propostas').select('*').eq('cnpj_raiz', cnpj).order('criado_em', { ascending: false }),
      supabase.from('contatos').select('*').eq('cnpj_raiz', cnpj).order('criado_em', { ascending: false }),
      supabase.from('eventos').select('id,tipo,numero_processo,ocorrido_em,payload,texto,advogados,link_publicacao')
        .eq('cnpj_raiz', cnpj).eq('fonte', 'JUDICIAL')
        .order('ocorrido_em', { ascending: false }).limit(80),
      supabase.from('vw_fila_oportunidades').select('score,alvo_marinheiro')
        .eq('cnpj_raiz', cnpj).order('score', { ascending: false }).limit(1),
      // Fase 6: alertas da Central para esta empresa (validação primeiro)
      supabase.from('alertas').select('*').eq('cnpj_raiz', cnpj)
        .order('detectado_em', { ascending: false }).limit(10),
    ])
    setEmpresa(emp.data)
    setInscricoes(ins.data || [])
    setInteracoes(int.data || [])
    setConsultas(con.data || [])
    setPropostas(prop.data || [])
    setContatos(cont.data || [])
    setEventos((evt.data as EventoJudicial[]) || [])
    setAlertas((alr.data as AlertaResumo[]) || [])
    const fila = (fil.data as { score: number | null; alvo_marinheiro: boolean }[] | null)?.[0]
    setScore(fila?.score ?? emp.data?.score_vf ?? null)
    setAlvoMarinheiro(fila?.alvo_marinheiro ?? false)
    setLoading(false)
  }

  async function updateEstagio(novoEstagio: PipelineStage) {
    if (!empresa) return
    // Fechar exige o desfecho: abre o modal em vez de gravar direto.
    if (novoEstagio === 'fechado') { setDesfechoOpen(true); return }
    setSaving(true)
    const patch = {
      estagio: novoEstagio,
      desfecho: null, tipo_fechamento: null, motivo_encerramento: null, motivo_obs: null, fechado_em: null,
      atualizado_em: new Date().toISOString(),
    }
    await supabase.from('empresas').update(patch).eq('cnpj_raiz', cnpj)
    setEmpresa(e => e ? { ...e, ...patch } : null)
    setSaving(false)
  }

  async function confirmarDesfecho(p: DesfechoPayload) {
    if (!empresa) return
    setSaving(true)
    const patch = {
      estagio: 'fechado' as PipelineStage,
      desfecho: p.desfecho,
      tipo_fechamento: p.tipo_fechamento,
      motivo_encerramento: p.motivo_encerramento,
      motivo_obs: p.motivo_obs,
      fechado_em: new Date().toISOString(),
      atualizado_em: new Date().toISOString(),
    }
    await supabase.from('empresas').update(patch).eq('cnpj_raiz', cnpj)
    setEmpresa(e => e ? { ...e, ...patch } : null)
    setDesfechoOpen(false)
    setSaving(false)
  }

  async function registrarInteracao() {
    if (!novaInteracao.resumo.trim()) return
    setSaving(true)
    const { data: { user } } = await supabase.auth.getUser()
    await supabase.from('interacoes').insert({
      cnpj_raiz: cnpj,
      canal: novaInteracao.canal,
      resumo: novaInteracao.resumo,
      proxima_acao: novaInteracao.proxima_acao || null,
      proxima_acao_em: novaInteracao.proxima_acao_em ? new Date(novaInteracao.proxima_acao_em).toISOString() : null,
      estagio_na_interacao: empresa?.estagio,
      criado_por: user?.id,
    })
    if (novaInteracao.proxima_acao_em) {
      await supabase.from('empresas').update({
        ultimo_contato_em: new Date().toISOString(),
        proxima_acao_em: new Date(novaInteracao.proxima_acao_em).toISOString(),
        proxima_acao_descricao: novaInteracao.proxima_acao,
        atualizado_em: new Date().toISOString(),
      }).eq('cnpj_raiz', cnpj)
    }
    setNovaInteracao({ canal: 'TELEFONE', resumo: '', proxima_acao: '', proxima_acao_em: '' })
    setShowInteracaoForm(false)
    await fetchAll()
    setSaving(false)
  }

  async function gerarDossie() {
    setDossieLoading(true); setDossieMsg(null)
    try {
      const url = `${process.env.NEXT_PUBLIC_SUPABASE_URL}/functions/v1/gerar-dossie`
      const { data: { session } } = await supabase.auth.getSession()
      const r = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          apikey: process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY as string,
          Authorization: `Bearer ${session?.access_token ?? process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY}`,
        },
        body: JSON.stringify({ cnpj_raiz: cnpj }),
      })
      const j = await r.json()
      if (j.ok) {
        setDossieMsg(`Dossiê gerado: ${j.decisores_criados} decisor(es) do QSA da Receita${j.contato_cadastral ? ' + contato cadastral' : ''}. Veja em Decisores e na aba Contatos — todos para validação.`)
        await fetchAll()
      } else {
        setDossieMsg(`Não foi possível gerar: ${j.erro ?? 'fonte indisponível'}`)
      }
    } catch (e: any) {
      setDossieMsg(`Falha ao chamar o dossiê: ${e?.message ?? 'erro de rede'}`)
    }
    setDossieLoading(false)
  }

  async function adicionarContato() {
    if (!novoContato.nome.trim()) return
    setSaving(true)
    const { data: { user } } = await supabase.auth.getUser()
    await supabase.from('contatos').insert({
      cnpj_raiz: cnpj,
      nome: novoContato.nome,
      cargo: novoContato.cargo || null,
      telefone: novoContato.telefone || null,
      email: novoContato.email || null,
      origem: 'MANUAL',
      criado_por: user?.id,
    })
    setNovoContato({ nome: '', cargo: '', telefone: '', email: '' })
    setShowContatoForm(false)
    await fetchAll()
    setSaving(false)
  }

  async function excluirContato(id: string) {
    setSaving(true)
    await supabase.from('contatos').delete().eq('id', id)
    await fetchAll()
    setSaving(false)
  }

  if (loading) {
    return <div className="flex items-center justify-center h-screen text-text-muted">Carregando painel…</div>
  }
  if (!empresa) {
    return <div className="p-8 text-danger">Empresa não encontrada: {cnpj}</div>
  }

  const motor = empresa.motor as MotorTipo | null
  const zona = empresa.zona_risco ?? null
  const evFiscais = eventos.filter(e => (e.payload?.ramo ?? 'OUTRO') !== 'TRABALHISTA')
  const evTrab = eventos.filter(e => e.payload?.ramo === 'TRABALHISTA')
  const tiposFiscais = new Set(evFiscais.map(e => e.tipo))
  // Advogados dos autos (DJEN) — canal real de abordagem quando nao ha decisor
  const advogados = (() => {
    const m = new Map<string, AdvogadoDJEN>()
    for (const e of [...evFiscais, ...evTrab]) for (const a of (e.advogados || [])) {
      if (a?.nome) m.set(`${a.nome}|${a.oab ?? ''}`, a)
    }
    return Array.from(m.values())
  })()
  const ratio = empresa.capital_social ? (empresa.valor_total_devida / empresa.capital_social) : null
  const socios = Array.isArray(empresa.socios) ? empresa.socios : []
  const dEvt = diasDe(empresa.evento_judicial_em)

  // Leitura de risco — regras do "mapa de guerra"
  const notasRisco: string[] = []
  if (tiposFiscais.has('SISBAJUD'))
    notasRisco.push('Bloqueio de conta (SISBAJUD) em processo fiscal — o seguro garantia substitui a constrição e libera o capital de giro.')
  if (tiposFiscais.has('PENHORA'))
    notasRisco.push('Penhora fiscal em curso — substituição por seguro garantia libera o ativo imobilizado.')
  if (tiposFiscais.has('EMBARGOS_EXEC'))
    notasRisco.push('Embargos à execução EXIGEM garantia do juízo (Lei 6.830) — jurisprudência aceita seguro garantia.')
  if (tiposFiscais.has('MANDADO_SEGURANCA'))
    notasRisco.push('Mandado de segurança NÃO substitui garantia: depende de liminar; se cair, a execução volta.')
  if (tiposFiscais.has('EXECUCAO_FISCAL'))
    notasRisco.push('Execução fiscal em andamento — a citação liga o cronômetro do risco de bloqueio.')

  // ---- Inteligência derivada (o "volante": cada item existe para uma decisão) ----
  const temDecisor = !!empresa.decisor_nome

  // Próxima ação: o comando no topo do cockpit, não um log
  const eventoAlertaRecente = dEvt != null && dEvt <= 20 &&
    (tiposFiscais.has('SISBAJUD') || tiposFiscais.has('PENHORA') || zona === 'SUFOCO')
  const acao: { critico: boolean; titulo: string; texto: string; sub: string } = (() => {
    if (eventoAlertaRecente) {
      const ev = empresa.evento_judicial_tipo
        ? (EVENTO_JUDICIAL_LABELS[empresa.evento_judicial_tipo] ?? empresa.evento_judicial_tipo)
        : 'Constrição'
      return {
        critico: true, titulo: 'Janela crítica', texto: 'Simular garantia e iniciar abordagem',
        sub: `${ev} há ${dEvt === 0 ? 'menos de 1 dia' : dEvt + ' dias'} — o seguro garantia substitui a constrição e libera o caixa.`,
      }
    }
    if (empresa.proxima_acao_descricao) {
      return {
        critico: false, titulo: 'Próxima ação agendada', texto: empresa.proxima_acao_descricao,
        sub: empresa.proxima_acao_em ? `Prazo: ${formatDate(empresa.proxima_acao_em)}` : '',
      }
    }
    if (motor === 'A2') {
      return {
        critico: false, titulo: 'Janela de prevenção', texto: 'Estruturar garantia antes do ajuizamento',
        sub: 'Sem execução localizada — abordagem consultiva (A2).',
      }
    }
    return {
      critico: false, titulo: 'Próximo passo',
      texto: temDecisor ? 'Iniciar abordagem ao decisor' : 'Mapear decisor e iniciar abordagem', sub: '',
    }
  })()

  // Seguradora: SEM direcionamento automático (decisão 19/08 — olhamos o mercado caso a caso)
  const seguradoraRazao = 'Sem direcionamento automático — a seguradora é escolhida olhando o mercado, caso a caso, na estruturação da proposta.'

  return (
    <div className="flex flex-col min-h-screen">

      {/* ===== Header ===== */}
      <div className="px-6 py-4 border-b border-border bg-bg-secondary">
        <div className="flex items-center gap-2 mb-3 flex-wrap">
          <Link href="/oportunidades" className="btn-ghost py-1 text-xs">
            <ArrowLeft className="w-3.5 h-3.5" /> Fila
          </Link>
          {zona && (
            <span className={cn('badge', ZONA_COLORS[zona] ?? 'bg-bg-hover text-text-muted')}>
              {zona === 'SUFOCO' && <ShieldAlert className="w-3 h-3 mr-1" />}
              ZONA {ZONA_LABELS[zona]?.toUpperCase() ?? zona}
            </span>
          )}
          {motor && (
            <span className={cn('badge', MOTOR_COLORS[motor])}>{MOTOR_BADGE_LABEL[motor]}</span>
          )}
          {empresa.tributo_principal && (
            <span className="badge bg-bg-hover text-text-muted">{empresa.tributo_principal}</span>
          )}
          {ratio != null && (
            <span className={cn('badge',
              ratio <= 2 ? 'bg-success/15 text-success' : ratio <= 5 ? 'bg-warning/15 text-warning' : 'bg-danger/15 text-danger')}
              title="Dívida ÷ Capital — quanto menor, mais segurável">
              D/C {ratio.toFixed(1)}×
            </span>
          )}
        </div>

        <div className="flex items-start justify-between gap-4">
          <div>
            <div className="flex items-center gap-2.5">
              <h1 className="text-xl font-bold text-text-primary">{empresa.nome_devedor}</h1>
              {alvoMarinheiro && (
                <span className="badge bg-rose/12 text-rose-light border border-rose/25 gap-1">
                  <Target className="w-3 h-3" /> Marinheiro
                </span>
              )}
            </div>
            <p className="text-text-muted text-sm">
              {formatCnpj(empresa.cnpj_completo)} · {empresa.uf_devedor}
              {empresa.capital_social ? <> · Capital {formatBRLCompact(empresa.capital_social)}</> : null}
            </p>
          </div>
          <div className="flex items-center gap-5">
            <div className="text-right">
              <p className="text-2xl font-bold text-rose-light">{formatBRLCompact(empresa.valor_total_devida)}</p>
              <p className="text-text-muted text-xs">{empresa.qtd_inscricoes} inscrição(ões) PGFN</p>
            </div>
            {score != null && <ScoreGauge score={score} />}
          </div>
        </div>

        {/* Estágio */}
        <div className="flex items-center gap-3 mt-3">
          <span className="text-text-faint text-xs">Estágio:</span>
          <div className="relative">
            <select
              className="select py-1 text-xs pr-8 appearance-none"
              value={normalizeStage(empresa.estagio)}
              onChange={e => updateEstagio(e.target.value as PipelineStage)}
              disabled={saving}
            >
              {STAGES_ORDERED.map(s => <option key={s} value={s}>{STAGE_LABELS[s]}</option>)}
            </select>
            <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 w-3 h-3 text-text-faint pointer-events-none" />
          </div>

          {/* Desfecho — só quando Fechado; clicável para revisar */}
          {normalizeStage(empresa.estagio) === 'fechado' && empresa.desfecho && (
            <button onClick={() => setDesfechoOpen(true)} className="inline-flex items-center gap-1.5" title="Revisar desfecho">
              <span className={cn('badge text-[10px]', DESFECHO_COLORS[empresa.desfecho])}>
                {DESFECHO_LABELS[empresa.desfecho]}
              </span>
              {empresa.desfecho === 'GANHO' && empresa.tipo_fechamento && (
                <span className="text-text-faint text-[10px]">{TIPO_FECHAMENTO_LABELS[empresa.tipo_fechamento]}</span>
              )}
              {empresa.desfecho !== 'GANHO' && empresa.motivo_encerramento && (
                <span className="text-text-faint text-[10px] truncate max-w-[160px]">{empresa.motivo_encerramento}</span>
              )}
            </button>
          )}

          {saving && <span className="text-text-faint text-xs">Salvando…</span>}
        </div>
      </div>

      {/* Modal de desfecho */}
      {desfechoOpen && (
        <DesfechoModal
          empresaNome={empresa.nome_devedor}
          initial={{
            desfecho: empresa.desfecho ?? undefined,
            tipo_fechamento: empresa.tipo_fechamento ?? undefined,
            motivo_encerramento: empresa.motivo_encerramento ?? undefined,
            motivo_obs: empresa.motivo_obs ?? undefined,
          }}
          saving={saving}
          onConfirm={confirmarDesfecho}
          onCancel={() => setDesfechoOpen(false)}
        />
      )}

      {/* ===== Tabs ===== */}
      <div className="border-b border-border bg-bg-secondary px-6">
        <div className="flex gap-0">
          {([
            { id: 'painel',      label: 'Painel',      icon: Target },
            { id: 'inscricoes',  label: `Inscrições (${inscricoes.length})`, icon: FileText },
            { id: 'contatos',    label: `Contatos (${contatos.length})`, icon: User },
            { id: 'interacoes',  label: `Interações (${interacoes.length})`, icon: MessageSquare },
            { id: 'proposta',    label: 'Proposta',    icon: Calculator },
          ] as const).map(({ id, label, icon: Icon }) => (
            <button
              key={id}
              onClick={() => setTab(id as Tab)}
              className={cn(
                'flex items-center gap-1.5 px-4 py-3 text-xs font-medium border-b-2 transition-colors',
                tab === id
                  ? 'border-vf-red text-vf-red-light'
                  : 'border-transparent text-text-muted hover:text-text-primary'
              )}
            >
              <Icon className="w-3.5 h-3.5" /> {label}
            </button>
          ))}
        </div>
      </div>

      <div className="flex-1 p-6 overflow-auto">

        {/* ================= PAINEL DA OPORTUNIDADE ================= */}
        {tab === 'painel' && (
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 max-w-6xl">

            {/* Próxima ação — o comando no topo do volante */}
            <div className={cn(
              'lg:col-span-3 rounded-xl border px-5 py-4 flex items-center gap-4',
              acao.critico
                ? 'border-danger/40 bg-gradient-to-r from-danger/10 to-transparent'
                : 'border-rose/30 bg-gradient-to-r from-rose/10 to-transparent'
            )}>
              <div className={cn(
                'w-10 h-10 rounded-lg grid place-items-center flex-shrink-0 border',
                acao.critico ? 'bg-danger/15 text-danger border-danger/40' : 'bg-rose/12 text-rose-light border-rose/30'
              )}>
                <Zap className="w-5 h-5" />
              </div>
              <div className="flex-1 min-w-0">
                <p className={cn('text-[10px] font-bold uppercase tracking-widest', acao.critico ? 'text-danger' : 'text-rose-light')}>
                  {acao.titulo}
                </p>
                <p className="text-text-primary font-semibold text-sm mt-0.5">
                  {acao.texto}
                  {acao.sub && <span className="text-text-muted font-normal"> — {acao.sub}</span>}
                </p>
              </div>
              <div className="flex flex-col sm:flex-row gap-2 flex-shrink-0">
                <button onClick={gerarDossie} disabled={dossieLoading}
                  className="btn-secondary text-xs" title="Puxa sócios/administradores e contato cadastral da Receita">
                  <Sparkles className="w-3.5 h-3.5" /> {dossieLoading ? 'Gerando…' : 'Gerar dossiê'}
                </button>
                <Link href={`/solver?cnpj=${cnpj}`} className="btn-primary text-xs">
                  Iniciar simulação <ArrowRight className="w-3.5 h-3.5" />
                </Link>
              </div>
            </div>
            {dossieMsg && (
              <div className="lg:col-span-3 -mt-3 text-xs px-1 text-success">{dossieMsg}</div>
            )}

            {/* ===== POR QUE ESTA EMPRESA PODE SER UMA OPORTUNIDADE AGORA? (Fase 6) ===== */}
            <div className="lg:col-span-3 card border-l-2 border-l-info">
              <p className="section-header flex items-center gap-2">
                <Zap className="w-3.5 h-3.5 text-info" /> Por que esta empresa pode ser uma oportunidade agora?
              </p>
              {alertas.length === 0 ? (
                <p className="text-text-muted text-xs">
                  Nenhum alerta da Central para esta empresa. A leitura abaixo vem do acervo
                  (eventos históricos) — <span className="text-warning">sem validação humana registrada</span>.
                </p>
              ) : (
                <div className="space-y-2.5">
                  {alertas.slice(0, 3).map(a => (
                    <div key={a.id} className="flex items-start gap-3 text-xs">
                      <span className={cn('badge text-[10px] flex-shrink-0 mt-0.5',
                        a.estado === 'VALIDADO' ? 'bg-success/15 text-success'
                        : a.estado === 'CONFLITO' ? 'bg-danger/15 text-danger'
                        : a.estado === 'DESCARTADO' ? 'bg-bg-hover text-text-faint'
                        : 'bg-info/15 text-info')}>
                        {a.estado.replace('_', ' ').toLowerCase()}
                      </span>
                      <div className="min-w-0">
                        <p className="text-text-primary">
                          <b>Fato:</b> {a.titulo.replace(/^Pauta 20\/08 · /, '')}
                          {a.numero_processo && <span className="text-text-faint font-mono"> · {a.numero_processo}</span>}
                        </p>
                        <p className="text-text-muted mt-0.5">
                          Evidência: <span className="font-medium">{a.evidencia_condicao}</span>
                          {' '}· papel: <span className={a.papel_processual === 'CREDORA' ? 'text-danger font-semibold' : ''}>{a.papel_processual}</span>
                          {(a.pendencias?.length ?? 0) > 0 && (
                            <span className="text-warning"> · falta confirmar: {(a.pendencias ?? []).length} item(ns)</span>
                          )}
                        </p>
                        <p className="text-text-muted mt-0.5">
                          <b className="text-text-primary">Interpretação:</b>{' '}
                          {a.papel_processual === 'CREDORA'
                            ? 'empresa é CREDORA nesta publicação — o gatilho fiscal dela está em outros autos; não abordar por este processo.'
                            : a.estado === 'VALIDADO'
                              ? 'fato validado — apta a abordagem consultiva sobre garantia.'
                              : 'candidato: conferir a fonte na Central antes de abordar.'}
                        </p>
                      </div>
                    </div>
                  ))}
                  <div className="pt-2 border-t border-border flex items-center justify-between">
                    <p className="text-[11px] text-text-faint">
                      Recomendação: {alertas.some(a => a.estado === 'VALIDADO')
                        ? 'preparar abordagem consultiva (há fato validado).'
                        : 'submeter à validação na Central antes de qualquer contato.'}
                    </p>
                    <Link href="/pauta" className="text-xs text-info hover:underline flex-shrink-0">
                      Validar na Central →
                    </Link>
                  </div>
                </div>
              )}
            </div>

            {/* Leitura de risco */}
            <div className="card lg:col-span-2 border-l-2 border-l-vf-red">
              <p className="section-header flex items-center gap-2">
                <ShieldAlert className="w-3.5 h-3.5 text-vf-red-light" /> Leitura de risco
              </p>
              {zona && (
                <p className="text-text-primary text-sm mb-2">
                  <span className={cn('badge mr-2', ZONA_COLORS[zona])}>{ZONA_LABELS[zona]}</span>
                  {ZONA_DESCRICAO[zona]}
                  {zona === 'SUFOCO' && dEvt != null && (
                    <span className="text-danger font-semibold"> Última constrição há {dEvt === 0 ? 'menos de 1 dia' : `${dEvt} dias`}.</span>
                  )}
                </p>
              )}
              {notasRisco.length > 0 ? (
                <ul className="space-y-1.5 mt-3">
                  {notasRisco.map((n, i) => (
                    <li key={i} className="text-xs text-text-muted flex gap-2">
                      <span className="text-vf-red-light">▸</span> {n}
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="text-text-faint text-xs mt-2">
                  Nenhum processo fiscal localizado nos diários — janela de prevenção (A2): estruturar garantia antes do ajuizamento.
                </p>
              )}
            </div>

            {/* Quem abordar */}
            <div className="card">
              <p className="section-header flex items-center gap-2">
                <User className="w-3.5 h-3.5 text-vf-red-light" /> Quem abordar
              </p>

              {/* Decisor principal (colunas decisor_* da empresa) */}
              {temDecisor && (
                <div className="mb-3 pb-3 border-b border-border">
                  <div className="flex items-center gap-2 flex-wrap">
                    <p className="text-text-primary font-semibold text-sm">{empresa.decisor_nome}</p>
                    <span className="badge bg-rose/12 text-rose-light text-[10px]">Decisor</span>
                  </div>
                  {empresa.decisor_cargo && <p className="text-text-muted text-xs">{empresa.decisor_cargo}</p>}
                  <ChannelRow tel={empresa.decisor_telefone} email={empresa.decisor_email} linkedin={empresa.decisor_linkedin} />
                </div>
              )}

              {/* Contatos cadastrados */}
              {contatos.length > 0 ? (
                <div className="space-y-2.5">
                  {contatos.slice(0, 3).map(c => (
                    <div key={c.id}>
                      <p className="text-text-primary font-medium text-sm">
                        {c.nome}
                        {c.cargo && <span className="text-text-muted font-normal text-xs"> · {c.cargo}</span>}
                      </p>
                      <ChannelRow tel={c.telefone} email={c.email} linkedin={c.linkedin} />
                    </div>
                  ))}
                </div>
              ) : !temDecisor ? (
                <>
                  <p className="text-text-faint text-xs mb-2">
                    Sem decisor mapeado — slot pronto para enriquecimento (Econodata).
                  </p>
                  {socios.length > 0 && (
                    <div className="space-y-1 mt-2">
                      <p className="text-text-faint text-[10px] uppercase tracking-wider">Na ausência, sócios do QSA</p>
                      {socios.slice(0, 3).map((s, i) => (
                        <p key={i} className="text-text-muted text-xs">
                          {s?.nome ?? String(s)}
                          {s?.qualificacao && <span className="text-text-faint"> · {s.qualificacao}</span>}
                        </p>
                      ))}
                    </div>
                  )}
                </>
              ) : null}

              {advogados.length > 0 && (
                <div className="mt-3 pt-3 border-t border-border">
                  <p className="text-text-faint text-[10px] uppercase tracking-wider mb-1.5 flex items-center gap-1">
                    <Gavel className="w-3 h-3" /> Advogados nos autos · DJEN
                  </p>
                  <div className="space-y-1.5 max-h-40 overflow-y-auto">
                    {advogados.map((a, i) => (
                      <div key={i} className="text-xs">
                        <p className="text-text-primary">{a.nome}</p>
                        {a.oab && <p className="text-text-faint text-[10px] font-mono">OAB {a.oab}{a.uf ? `/${a.uf}` : ''}</p>}
                      </div>
                    ))}
                  </div>
                  <p className="text-text-faint text-[10px] mt-1.5 leading-relaxed">
                    Advogado da executada = porta de entrada técnica: já conhece a execução e fala a língua da garantia.
                  </p>
                </div>
              )}

              <button onClick={() => setTab('contatos')} className="btn-secondary text-xs mt-3 w-full justify-center">
                <Plus className="w-3 h-3" /> Gerenciar contatos
              </button>
            </div>

            {/* Timeline fiscal */}
            <div className="card lg:col-span-2">
              <p className="section-header flex items-center gap-2">
                <Landmark className="w-3.5 h-3.5 text-vf-red-light" /> Linha do tempo — processos fiscais ({evFiscais.length})
              </p>
              {evFiscais.length === 0 ? (
                <p className="text-text-faint text-xs">Nenhum evento fiscal capturado nos diários oficiais.</p>
              ) : (
                <div className="relative max-h-96 overflow-y-auto pr-1 pl-1">
                  {/* trilho vertical */}
                  <div className="absolute left-[86px] top-2 bottom-2 w-px bg-border" />
                  {evFiscais.slice(0, 40).map((ev, i) => {
                    const alerta = EVENTO_ALERTA_MAXIMO.has(ev.tipo)
                    const dias = diasDe(ev.ocorrido_em)
                    return (
                      <div key={ev.id} className="relative flex gap-3 py-2.5">
                        <div className="flex-shrink-0 w-[74px] text-right pt-0.5">
                          <p className="text-text-muted text-[11px] font-medium tabular-nums">{formatDate(ev.ocorrido_em)}</p>
                          {dias != null && (
                            <p className="text-text-faint text-[10px]">{dias === 0 ? 'hoje' : `há ${dias}d`}</p>
                          )}
                        </div>
                        {/* nó */}
                        <div className="relative flex-shrink-0 w-3 flex justify-center pt-1">
                          {alerta ? (
                            <span className="relative flex h-2.5 w-2.5">
                              <span className="absolute inline-flex h-full w-full rounded-full bg-danger opacity-60 animate-ping" />
                              <span className="relative inline-flex rounded-full h-2.5 w-2.5 bg-danger shadow-[0_0_8px_2px_rgba(240,97,107,.55)]" />
                            </span>
                          ) : (
                            <span className="inline-flex rounded-full h-2 w-2 bg-rose shadow-[0_0_6px_1px_var(--accent-glow)] mt-0.5" />
                          )}
                        </div>
                        <div className="min-w-0 flex-1 pb-0.5">
                          <p className={cn('text-xs font-semibold leading-snug', alerta ? 'text-danger' : 'text-text-primary')}>
                            {EVENTO_JUDICIAL_LABELS[ev.tipo] ?? ev.tipo}
                          </p>
                          <div className="flex flex-wrap items-center gap-1.5 mt-1">
                            <span className="inline-flex items-center gap-1 text-[10px] font-medium text-rose-light bg-rose/10 border border-rose/20 rounded px-1.5 py-0.5">
                              <CheckCircle className="w-2.5 h-2.5" /> DJEN{ev.payload?.tribunal ? ` · ${ev.payload.tribunal}` : ''}
                            </span>
                            {ev.payload?.polo === 'A' && (
                              <span className="text-[10px] text-info bg-info/10 border border-info/20 rounded px-1.5 py-0.5">polo ativo</span>
                            )}
                            {ev.payload?.polo === 'P' && (
                              <span className="text-[10px] text-warning bg-warning/10 border border-warning/20 rounded px-1.5 py-0.5">polo passivo · executada</span>
                            )}
                            {ev.numero_processo && (
                              <span className="text-[10px] font-mono text-text-faint truncate max-w-[220px]">{ev.numero_processo}</span>
                            )}
                          </div>
                          {ev.texto && (
                            <details className="mt-1.5">
                              <summary className="cursor-pointer select-none text-[10px] font-medium text-rose-light/80 hover:text-rose-light list-none inline-flex items-center gap-1">
                                <ChevronDown className="w-3 h-3" /> Trecho do diário
                              </summary>
                              <blockquote className="mt-1.5 pl-3 border-l-2 border-rose/40 text-[11px] leading-relaxed text-text-muted bg-bg-secondary/60 rounded-r py-2 pr-2">
                                {ev.texto}
                              </blockquote>
                              {ev.link_publicacao && (
                                <a href={ev.link_publicacao} target="_blank" rel="noreferrer"
                                  className="inline-block mt-1 text-[10px] text-info hover:underline">
                                  Ver comunicação no DJEN ↗
                                </a>
                              )}
                            </details>
                          )}
                        </div>
                      </div>
                    )
                  })}
                </div>
              )}
            </div>

            {/* Coluna direita: trabalhista + sócios + cadastro */}
            <div className="space-y-6">

              {/* Seguradora — sem direcionamento automático; decisão de mercado */}
              <div className="card">
                <p className="section-header flex items-center gap-2">
                  <Shield className="w-3.5 h-3.5 text-info" /> Seguradora
                </p>
                <p className="text-text-muted text-xs leading-relaxed">{seguradoraRazao}</p>
              </div>

              {/* Trabalhista — separado, sem relação securitária */}
              <div className="card">
                <p className="section-header flex items-center gap-2">
                  <Hammer className="w-3.5 h-3.5 text-text-faint" /> Trabalhista (separado)
                </p>
                {evTrab.length === 0 ? (
                  <p className="text-text-faint text-xs">Nenhuma constrição trabalhista capturada.</p>
                ) : (
                  <>
                    <p className="text-text-primary text-sm font-semibold">{evTrab.length} constrição(ões)</p>
                    <p className="text-text-muted text-xs">Última: {formatDate(empresa.evento_trabalhista_em ?? evTrab[0]?.ocorrido_em)}</p>
                  </>
                )}
                <p className="text-text-faint text-[10px] mt-2 leading-relaxed">
                  Sem relação com o seguro garantia tributário — usado apenas como sinal de estresse de caixa.
                </p>
              </div>

              {/* Sócios (QSA) */}
              <div className="card">
                <p className="section-header flex items-center gap-2">
                  <Users className="w-3.5 h-3.5 text-vf-red-light" /> Sócios · QSA ({socios.length})
                </p>
                {socios.length === 0 ? (
                  <p className="text-text-faint text-xs">QSA não disponível.</p>
                ) : (
                  <div className="space-y-1.5 max-h-44 overflow-y-auto">
                    {socios.map((s, i) => (
                      <div key={i} className="text-xs">
                        <p className="text-text-primary">{s?.nome ?? String(s)}</p>
                        {s?.qualificacao && <p className="text-text-faint text-[10px]">{s.qualificacao}</p>}
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {/* Cadastro */}
              <div className="card">
                <p className="section-header">Cadastro</p>
                <dl className="space-y-2 text-sm">
                  <Row label="Situação CNPJ" value={empresa.cnpj_situacao || '—'} />
                  <Row label="Capital Social" value={empresa.capital_social ? formatBRL(empresa.capital_social) : '—'} />
                  <Row label="Tributo principal" value={empresa.tributo_principal || '—'} />
                  <Row label="Último contato" value={formatDateTime(empresa.ultimo_contato_em)} />
                  <Row label="Próxima ação" value={empresa.proxima_acao_descricao || '—'} />
                </dl>
              </div>
            </div>

            {/* Ações */}
            <div className="lg:col-span-3 flex gap-2">
              <Link href={`/solver?cnpj=${cnpj}`} className="btn-primary text-xs">
                <Calculator className="w-3.5 h-3.5" /> Estruturar proposta no VF Solver
              </Link>
              <button onClick={() => { setTab('interacoes'); setShowInteracaoForm(true) }} className="btn-secondary text-xs">
                <MessageSquare className="w-3.5 h-3.5" /> Registrar contato
              </button>
            </div>
          </div>
        )}

        {/* ================= INSCRIÇÕES ================= */}
        {tab === 'inscricoes' && (
          <div>
            <div className="flex items-center justify-between mb-4">
              <p className="text-text-primary font-medium">
                {inscricoes.length} inscrições — Total: {formatBRL(inscricoes.reduce((s, i) => s + (i.valor_numerico || 0), 0))}
              </p>
            </div>
            <div className="overflow-auto">
              <table className="table-vf">
                <thead>
                  <tr>
                    <th>Nº Inscrição</th><th>Tributo</th><th>Situação</th><th>Motor</th>
                    <th>Data</th><th>Dias</th><th className="text-right">Valor</th><th>Garantia</th><th>Ajuizado</th>
                  </tr>
                </thead>
                <tbody>
                  {inscricoes.map(ins => (
                    <tr key={ins.id}>
                      <td className="font-mono text-xs text-text-muted">{ins.numero_inscricao}</td>
                      <td><span className="badge bg-bg-secondary text-text-muted text-[10px]">{ins.tributo || '—'}</span></td>
                      <td className="text-xs text-text-muted max-w-36 truncate">{ins.situacao_inscricao}</td>
                      <td>{ins.motor && <span className={cn('badge text-[10px]', MOTOR_COLORS[ins.motor as MotorTipo])}>{ins.motor}</span>}</td>
                      <td className="text-xs text-text-muted">{formatDate(ins.data_inscricao)}</td>
                      <td className="text-xs text-text-muted">{ins.dias_inscricao ?? '—'}</td>
                      <td className="text-right font-semibold text-text-primary">{formatBRL(ins.valor_numerico)}</td>
                      <td className="text-xs text-text-muted">{ins.tipo_garantia}</td>
                      <td>{ins.indicador_ajuizado
                        ? <CheckCircle className="w-3.5 h-3.5 text-danger" />
                        : <span className="text-text-faint text-xs">Não</span>}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* ================= CONTATOS ================= */}
        {tab === 'contatos' && (
          <div className="max-w-3xl">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-sm font-semibold text-text-primary">Contatos / Decisores</h3>
              <button onClick={() => setShowContatoForm(!showContatoForm)} className="btn-primary text-xs">
                <Plus className="w-3.5 h-3.5" /> Adicionar contato
              </button>
            </div>

            {showContatoForm && (
              <div className="card mb-4 space-y-3">
                <div className="grid grid-cols-2 gap-3">
                  <input placeholder="Nome *" value={novoContato.nome}
                    onChange={e => setNovoContato({ ...novoContato, nome: e.target.value })} className="input text-sm" />
                  <input placeholder="Cargo" value={novoContato.cargo}
                    onChange={e => setNovoContato({ ...novoContato, cargo: e.target.value })} className="input text-sm" />
                  <input placeholder="Telefone" value={novoContato.telefone}
                    onChange={e => setNovoContato({ ...novoContato, telefone: e.target.value })} className="input text-sm" />
                  <input placeholder="E-mail" value={novoContato.email}
                    onChange={e => setNovoContato({ ...novoContato, email: e.target.value })} className="input text-sm" />
                </div>
                <div className="flex gap-2">
                  <button onClick={adicionarContato} disabled={saving || !novoContato.nome.trim()} className="btn-primary text-xs">
                    {saving ? 'Salvando…' : 'Salvar'}
                  </button>
                  <button onClick={() => setShowContatoForm(false)} className="btn-secondary text-xs">Cancelar</button>
                </div>
              </div>
            )}

            {contatos.length === 0 ? (
              <p className="text-sm text-text-muted py-8 text-center">
                Nenhum contato ainda. Adicione manualmente ou enriqueça pela Econodata.
              </p>
            ) : (
              <div className="space-y-2">
                {contatos.map((c) => (
                  <div key={c.id} className="flex items-start justify-between p-3 bg-bg-card rounded-lg border border-border">
                    <div className="flex-1">
                      <div className="flex items-center gap-2">
                        <span className="text-sm font-medium text-text-primary">{c.nome}</span>
                        {c.cargo && <span className="text-xs text-text-muted">· {c.cargo}</span>}
                        {c.origem && c.origem !== 'MANUAL' && (
                          <span className="text-[10px] px-1.5 py-0.5 bg-bg-secondary rounded text-text-muted">{c.origem}</span>
                        )}
                      </div>
                      <div className="flex gap-4 mt-1">
                        {c.telefone && <span className="flex items-center gap-1 text-xs text-text-muted"><Phone className="w-3 h-3" /> {c.telefone}</span>}
                        {c.email && <span className="flex items-center gap-1 text-xs text-text-muted"><Mail className="w-3 h-3" /> {c.email}</span>}
                      </div>
                    </div>
                    <button onClick={() => excluirContato(c.id)} className="p-1.5 text-text-muted hover:text-danger transition-colors" title="Excluir contato">
                      <Trash2 className="w-4 h-4" />
                    </button>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {/* ================= INTERAÇÕES ================= */}
        {tab === 'interacoes' && (
          <div className="max-w-2xl">
            <div className="flex items-center justify-between mb-4">
              <p className="text-text-primary font-medium">Histórico de Contatos</p>
              <button onClick={() => setShowInteracaoForm(v => !v)} className="btn-primary text-xs">
                <Plus className="w-3.5 h-3.5" /> Nova interação
              </button>
            </div>

            {showInteracaoForm && (
              <div className="card mb-4 space-y-3">
                <p className="text-text-primary text-sm font-medium">Registrar contato</p>
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className="label">Canal</label>
                    <select className="select text-sm" value={novaInteracao.canal}
                      onChange={e => setNovaInteracao(n => ({ ...n, canal: e.target.value as CanalInteracao }))}>
                      {['EMAIL','TELEFONE','WHATSAPP','REUNIAO','SISTEMA'].map(c => <option key={c} value={c}>{c}</option>)}
                    </select>
                  </div>
                  <div>
                    <label className="label">Próxima ação — data</label>
                    <input type="datetime-local" className="input text-sm"
                      value={novaInteracao.proxima_acao_em}
                      onChange={e => setNovaInteracao(n => ({ ...n, proxima_acao_em: e.target.value }))} />
                  </div>
                </div>
                <div>
                  <label className="label">Resumo do contato *</label>
                  <textarea className="input text-sm resize-none" rows={3}
                    placeholder="Detalhe o que foi discutido…"
                    value={novaInteracao.resumo}
                    onChange={e => setNovaInteracao(n => ({ ...n, resumo: e.target.value }))} />
                </div>
                <div>
                  <label className="label">Próxima ação</label>
                  <input className="input text-sm" placeholder="Ex: Enviar proposta, Ligar para confirmar…"
                    value={novaInteracao.proxima_acao}
                    onChange={e => setNovaInteracao(n => ({ ...n, proxima_acao: e.target.value }))} />
                </div>
                <div className="flex gap-2">
                  <button onClick={registrarInteracao} disabled={!novaInteracao.resumo.trim() || saving} className="btn-primary text-xs">
                    {saving ? 'Salvando…' : 'Salvar'}
                  </button>
                  <button onClick={() => setShowInteracaoForm(false)} className="btn-secondary text-xs">Cancelar</button>
                </div>
              </div>
            )}

            <div className="space-y-3">
              {interacoes.length === 0 ? (
                <p className="text-text-faint text-sm">Nenhum contato registrado ainda.</p>
              ) : interacoes.map(int => (
                <div key={int.id} className="card flex gap-3">
                  <div className="flex-shrink-0 w-16 text-center">
                    <span className="badge bg-bg-secondary text-text-muted text-[10px]">{int.canal}</span>
                    <p className="text-text-faint text-[10px] mt-1">{formatDate(int.criado_em)}</p>
                  </div>
                  <div className="flex-1">
                    <p className="text-text-primary text-sm">{int.resumo}</p>
                    {int.proxima_acao && (
                      <p className="text-info text-xs mt-1">
                        <Clock className="w-3 h-3 inline mr-1" />
                        {int.proxima_acao} {int.proxima_acao_em ? `— ${formatDate(int.proxima_acao_em)}` : ''}
                      </p>
                    )}
                    <p className="text-text-faint text-[10px] mt-1">
                      {(int.responsavel as any)?.nome || 'Sistema'} · {int.estagio_na_interacao ? STAGE_LABELS[normalizeStage(int.estagio_na_interacao)] : ''}
                    </p>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* ================= PROPOSTA ================= */}
        {tab === 'proposta' && (
          <div className="max-w-3xl">
            <div className="flex items-center justify-between mb-4">
              <p className="text-text-primary font-medium">Propostas ({propostas.length})</p>
              <Link href={`/solver?cnpj=${cnpj}`} className="btn-primary text-xs">
                <Calculator className="w-3.5 h-3.5" /> Gerar no VF Solver
              </Link>
            </div>

            {propostas.length === 0 ? (
              <div className="card text-center py-8">
                <Calculator className="w-8 h-8 mx-auto mb-2 text-text-faint" />
                <p className="text-text-muted text-sm">Nenhuma proposta gerada.</p>
                <p className="text-text-faint text-xs mt-1">Use o VF Solver para calcular e registrar a proposta.</p>
              </div>
            ) : (
              <div className="space-y-3">
                {propostas.map(p => (
                  <div key={p.id} className="card">
                    <div className="flex justify-between items-start mb-3">
                      <div>
                        <p className="text-text-primary font-semibold">{formatBRL(p.valor_garantia)} de garantia</p>
                        <p className="text-text-muted text-xs">{p.seguradora} · {(p.taxa_anual * 100).toFixed(2)}% a.a. · {p.prazo_anos}a</p>
                      </div>
                      <span className={cn('badge text-xs',
                        p.status === 'CONVERTIDA' ? 'bg-success/20 text-success' :
                        p.status === 'APROVADA'   ? 'bg-info/20 text-info' :
                        p.status === 'RECUSADA'   ? 'bg-danger/20 text-danger' :
                        'bg-text-faint/20 text-text-muted'
                      )}>
                        {p.status}
                      </span>
                    </div>
                    <div className="grid grid-cols-4 gap-3 text-sm">
                      <div><p className="text-text-faint text-xs">Prêmio Bruto</p><p className="font-semibold">{formatBRL(p.premio_bruto)}</p></div>
                      <div><p className="text-text-faint text-xs">Comissão</p><p className="font-semibold text-success">{formatBRL(p.comissao_valor)}</p></div>
                      <div><p className="text-text-faint text-xs">Honorários</p><p className="font-semibold text-info">{formatBRL(p.honorarios_valor)}</p></div>
                      <div><p className="text-text-faint text-xs">Receita V&F</p><p className="font-bold text-vf-red-light">{formatBRL(p.receita_vf_total)}</p></div>
                    </div>
                    {p.regra_economica_ok === false && (
                      <div className="mt-2 text-xs text-warning flex items-center gap-1">
                        <AlertTriangle className="w-3 h-3" /> Regra econômica não atingida
                      </div>
                    )}
                    <p className="text-text-faint text-[10px] mt-2">{formatDateTime(p.criado_em)}</p>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex justify-between gap-4">
      <span className="text-text-faint">{label}</span>
      <span className="text-text-primary text-right">{value}</span>
    </div>
  )
}

function ScoreGauge({ score }: { score: number }) {
  const r = 30
  const c = 2 * Math.PI * r
  const pct = Math.max(0, Math.min(100, score)) / 100
  const offset = c * (1 - pct)
  const faixa = score >= 75 ? 'Quente' : score >= 50 ? 'Morno' : 'Frio'
  return (
    <div className="relative flex-shrink-0" style={{ width: 76, height: 76 }} title={`Score de oportunidade: ${score}/100 · ${faixa}`}>
      <svg width={76} height={76} viewBox="0 0 76 76" className="-rotate-90">
        <circle cx={38} cy={38} r={r} fill="none" stroke="var(--border-strong)" strokeWidth={5} />
        <circle
          cx={38} cy={38} r={r} fill="none"
          stroke="var(--accent)" strokeWidth={5} strokeLinecap="round"
          strokeDasharray={c} strokeDashoffset={offset}
          style={{ filter: 'drop-shadow(0 0 4px var(--accent-glow))', transition: 'stroke-dashoffset .6s ease' }}
        />
      </svg>
      <div className="absolute inset-0 flex flex-col items-center justify-center">
        <span className="text-lg font-bold leading-none text-rose-light tabular-nums">{score}</span>
        <span className="text-[8px] uppercase tracking-widest text-text-faint mt-0.5">score</span>
      </div>
    </div>
  )
}

// Canais de contato: acesos quando o dado existe, apagados quando falta (F1: cada luz é uma decisão)
function ChannelRow({ tel, email, linkedin }: { tel?: string | null; email?: string | null; linkedin?: string | null }) {
  return (
    <div className="flex items-center gap-1.5 mt-1.5 flex-wrap">
      <Channel on={!!tel} icon={<Phone className="w-3 h-3" />} label={tel || 'sem telefone'} href={tel ? `tel:${tel}` : undefined} />
      <Channel on={!!email} icon={<Mail className="w-3 h-3" />} label={email || 'sem e-mail'} href={email ? `mailto:${email}` : undefined} />
      <Channel on={!!linkedin} icon={<Linkedin className="w-3 h-3" />} label={linkedin ? 'LinkedIn' : 'sem LinkedIn'} href={linkedin || undefined} />
    </div>
  )
}

function Channel({ on, icon, label, href }: { on: boolean; icon: ReactNode; label: string; href?: string }) {
  const cls = cn(
    'inline-flex items-center gap-1 text-[10px] px-1.5 py-0.5 rounded border transition-colors',
    on ? 'text-rose-light border-rose/30 bg-rose/10 hover:bg-rose/20' : 'text-text-faint border-border'
  )
  if (on && href) {
    return <a href={href} target="_blank" rel="noreferrer" className={cls} title={label}>{icon}</a>
  }
  return <span className={cls} title={label}>{icon}</span>
}
