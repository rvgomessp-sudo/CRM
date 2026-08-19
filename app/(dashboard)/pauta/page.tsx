'use client'

/**
 * CENTRAL DE OPORTUNIDADES — Fase 5 da ordem executiva 19/08.
 * Substitui a "Pauta do dia".
 *
 * Regras que esta tela obedece (e a UI não deixa violar):
 * - Alerta NÃO é oportunidade: promoção só via fn_promover_alerta (exige VALIDADO).
 * - Credora nunca é tratada como devedora (papel processual explícito).
 * - Dívida PGFN e valor do processo são colunas distintas e rotuladas.
 * - Score legado aparece como "legado", nunca como autoridade.
 * - Pendências ("o que falta confirmar") sempre visíveis.
 * - Toda decisão grava autoria (validado_por) e vai à trilha (trigger).
 */

import { Suspense, useCallback, useEffect, useMemo, useState } from 'react'
import Link from 'next/link'
import { useRouter, useSearchParams } from 'next/navigation'
import { createClient } from '@/lib/supabase/client'
import { formatBRLCompact, formatDate, cn } from '@/lib/utils'
import type { FilaRow } from '@/lib/types'
import {
  Radar, CheckCircle2, XCircle, Eye, AlertTriangle, ShieldQuestion,
  ArrowUpRight, Search, ExternalLink, Scale, X, Clock,
} from 'lucide-react'

// ---------- tipos ----------
interface Alerta {
  id: string
  cnpj_raiz: string
  numero_processo: string | null
  titulo: string
  gravidade: 'CRITICO' | 'IMPORTANTE' | 'INFORMATIVO'
  estado: 'CANDIDATO' | 'AGUARDANDO_VALIDACAO' | 'VALIDADO' | 'CONFLITO' | 'DESCARTADO' | 'MONITORAR'
  evidencia_condicao: string
  papel_processual: string
  ramo: string | null
  valor_processo: number | null
  pendencias: string[]
  score_legado: number | null
  fonte: string | null
  link_fonte: string | null
  trecho: string | null
  padrao_playbook: string | null
  responsavel: 'RODRIGO' | 'ANA' | null
  detectado_em: string
  validado_em: string | null
  validacao_nota: string | null
}

const ESTADO_META: Record<Alerta['estado'], { label: string; cls: string }> = {
  CANDIDATO:            { label: 'Candidato',      cls: 'bg-info/15 text-info' },
  AGUARDANDO_VALIDACAO: { label: 'Aguard. valid.', cls: 'bg-warning/15 text-warning' },
  VALIDADO:             { label: 'Validado',       cls: 'bg-success/15 text-success' },
  CONFLITO:             { label: 'Conflito',       cls: 'bg-danger/15 text-danger' },
  DESCARTADO:           { label: 'Descartado',     cls: 'bg-bg-hover text-text-faint' },
  MONITORAR:            { label: 'Monitorar',      cls: 'bg-rose/15 text-rose-light' },
}
const EVID_META: Record<string, string> = {
  CONFIRMADO: 'text-success', CORROBORADO: 'text-info', HIPOTESE: 'text-warning',
  LACUNA: 'text-text-faint', CONFLITO: 'text-danger', DESATUALIZADO: 'text-text-faint',
  NAO_CONFIRMAVEL: 'text-text-faint',
}
const PAPEL_META: Record<string, { label: string; cls: string }> = {
  DEVEDORA:       { label: 'Devedora',  cls: 'text-text-primary' },
  CREDORA:        { label: 'CREDORA',   cls: 'text-danger font-semibold' },
  AMBIGUO:        { label: 'Ambíguo',   cls: 'text-warning' },
  PENDENTE:       { label: 'Pendente',  cls: 'text-warning' },
  NAO_CONFIRMADO: { label: 'Não conf.', cls: 'text-text-faint' },
}
const PENDENCIA_LABELS: Record<string, string> = {
  RETESTE_RECORRENCIA_PGFN:            'Reteste de recorrência PGFN',
  VALOR_PROCESSO_NAO_EXTRAIDO:         'Valor do processo não extraído',
  PAPEL_PROCESSUAL_PENDENTE:           'Papel processual pendente',
  EMPRESA_APARECE_COMO_CREDORA:        'Empresa aparece como CREDORA',
  GRUPO_ECONOMICO_NAO_CONFIRMADO:      'Grupo econômico não confirmado',
  FONTE_ORIGINAL_AUSENTE:              'Fonte original ausente',
  EVENTO_TRABALHISTA_NAO_E_DIVIDA_FISCAL: 'Evento trabalhista ≠ dívida fiscal',
  AVISO_DA_PAUTA_ORIGINAL:             'Aviso herdado da pauta',
  CORRIGIR_DOCUMENTO_PAUTA:            'Corrigir documento da pauta',
}

function proximaAcao(a: Alerta): string {
  if (a.estado === 'CANDIDATO')            return 'Conferir fonte e validar'
  if (a.estado === 'AGUARDANDO_VALIDACAO') return 'Concluir validação'
  if (a.estado === 'CONFLITO')             return 'Corrigir papel/identidade'
  if (a.estado === 'VALIDADO')             return 'Promover e preparar abordagem'
  if (a.estado === 'MONITORAR')            return 'Acompanhar movimentos'
  return '—'
}
function diasDe(d: string | null | undefined): number | null {
  if (!d) return null
  return Math.floor((Date.now() - new Date(d).getTime()) / 86400000)
}

export default function CentralPage() {
  return <Suspense><Central /></Suspense>
}

function Central() {
  const supabase = createClient()
  const router = useRouter()
  const sp = useSearchParams()

  // filtros persistem na URL
  const fEstado = sp.get('estado') ?? ''
  const fGrav   = sp.get('grav') ?? ''
  const fPapel  = sp.get('papel') ?? ''
  const fRamo   = sp.get('ramo') ?? ''
  const fResp   = sp.get('resp') ?? ''
  const q       = sp.get('q') ?? ''

  const [alertas, setAlertas] = useState<Alerta[]>([])
  const [fila, setFila] = useState<Map<string, FilaRow>>(new Map())
  const [loading, setLoading] = useState(true)
  const [busca, setBusca] = useState(q)
  const [painelId, setPainelId] = useState<string | null>(null)
  const [painelContatos, setPainelContatos] = useState<{ telefone: string | null; email: string | null; cargo: string | null }[]>([])
  const [painelAdv, setPainelAdv] = useState<{ nome: string; oab: string | null; uf: string | null } | null>(null)
  const [nota, setNota] = useState('')
  const [salvando, setSalvando] = useState(false)
  const [atualizadoEm, setAtualizadoEm] = useState<Date | null>(null)

  const setParam = useCallback((patch: Record<string, string | null>) => {
    const next = new URLSearchParams(Array.from(sp.entries()))
    for (const [k, v] of Object.entries(patch)) {
      if (!v) next.delete(k); else next.set(k, v)
    }
    router.replace(`/pauta?${next.toString()}`, { scroll: false })
  }, [router, sp])

  useEffect(() => {
    const t = setTimeout(() => { if (busca !== q) setParam({ q: busca || null }) }, 400)
    return () => clearTimeout(t)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [busca])

  const fetchAll = useCallback(async () => {
    setLoading(true)
    const { data: al } = await supabase.from('alertas').select('*')
      .order('detectado_em', { ascending: false }).limit(500)
    const lista = ((al ?? []) as any[]).map(a => ({ ...a, pendencias: a.pendencias ?? [] })) as Alerta[]
    setAlertas(lista)
    const cnpjs = [...new Set(lista.map(a => a.cnpj_raiz))].filter(c => c !== '00000000')
    if (cnpjs.length) {
      const { data: f } = await supabase.from('vw_fila_oportunidades').select('*').in('cnpj_raiz', cnpjs)
      setFila(new Map(((f ?? []) as FilaRow[]).map(r => [r.cnpj_raiz, r])))
    }
    setAtualizadoEm(new Date())
    setLoading(false)
  }, [supabase])
  useEffect(() => { fetchAll() }, [fetchAll])

  // ---------- ações (toda decisão grava autoria) ----------
  async function mudarEstado(a: Alerta, estado: Alerta['estado']) {
    setSalvando(true)
    const { data: { user } } = await supabase.auth.getUser()
    const { error } = await supabase.from('alertas').update({
      estado, validado_por: user?.id ?? null, validado_em: new Date().toISOString(),
      validacao_nota: nota || null,
    }).eq('id', a.id)
    if (!error) {
      setAlertas(prev => prev.map(x => x.id === a.id ? { ...x, estado, validacao_nota: nota || null } : x))
      setNota('')
    } else alert(`Falha ao gravar: ${error.message}`)
    setSalvando(false)
  }
  async function promover(a: Alerta) {
    setSalvando(true)
    const { data: { user } } = await supabase.auth.getUser()
    const { data, error } = await supabase.rpc('fn_promover_alerta', { p_alerta: a.id, p_autor: user?.id ?? null })
    if (error) alert(`Promoção recusada pelo banco: ${error.message}`)
    else alert(`Promovido. Oportunidade: ${data ?? 'sem fila (verificar)'}`)
    setSalvando(false)
    fetchAll()
  }

  // ---------- derivações ----------
  const filtrados = useMemo(() => alertas.filter(a => {
    if (fEstado && a.estado !== fEstado) return false
    if (fGrav && a.gravidade !== fGrav) return false
    if (fPapel && a.papel_processual !== fPapel) return false
    if (fRamo && a.ramo !== fRamo) return false
    if (fResp && a.responsavel !== fResp) return false
    if (q) {
      const nome = fila.get(a.cnpj_raiz)?.nome_devedor ?? a.titulo
      if (!nome.toLowerCase().includes(q.toLowerCase())) return false
    }
    return true
  }), [alertas, fila, fEstado, fGrav, fPapel, fRamo, fResp, q])

  const kpi = useMemo(() => ({
    candidatos:  alertas.filter(a => a.estado === 'CANDIDATO').length,
    aguardando:  alertas.filter(a => a.estado === 'AGUARDANDO_VALIDACAO').length,
    validados:   alertas.filter(a => a.estado === 'VALIDADO').length,
    conflitos:   alertas.filter(a => a.estado === 'CONFLITO').length,
    criticos:    alertas.filter(a => a.gravidade === 'CRITICO' && !['DESCARTADO'].includes(a.estado)).length,
    divida: alertas.filter(a => ['CANDIDATO','AGUARDANDO_VALIDACAO','VALIDADO'].includes(a.estado) && a.papel_processual !== 'CREDORA')
      .reduce((s, a) => s + (Number(fila.get(a.cnpj_raiz)?.valor_total_devida) || 0), 0),
  }), [alertas, fila])

  // Prioridades do topo: regra determinística e EXPLICADA no card —
  // crítico > evidência corroborada > mais recente; conflito/credora nunca priorizado.
  const prioridades = useMemo(() =>
    [...alertas]
      .filter(a => ['CANDIDATO','AGUARDANDO_VALIDACAO','VALIDADO'].includes(a.estado)
        && a.papel_processual !== 'CREDORA' && a.gravidade !== 'INFORMATIVO')
      .sort((x, y) =>
        (x.gravidade === 'CRITICO' ? 0 : 1) - (y.gravidade === 'CRITICO' ? 0 : 1)
        || (x.evidencia_condicao === 'CORROBORADO' ? 0 : 1) - (y.evidencia_condicao === 'CORROBORADO' ? 0 : 1)
        || new Date(y.detectado_em).getTime() - new Date(x.detectado_em).getTime())
      .slice(0, 4)
  , [alertas])

  const painel = painelId ? alertas.find(a => a.id === painelId) ?? null : null
  const painelFila = painel ? fila.get(painel.cnpj_raiz) : undefined

  // Abordagem sem caça: contato (Receita/manual) + advogado dos autos no painel
  useEffect(() => {
    setPainelContatos([]); setPainelAdv(null)
    if (!painel || painel.cnpj_raiz === '00000000') return
    supabase.from('contatos').select('telefone,email,cargo').eq('cnpj_raiz', painel.cnpj_raiz).limit(4)
      .then(({ data }) => setPainelContatos(data ?? []))
    if (painel.numero_processo) {
      supabase.from('eventos').select('advogados').eq('fonte', 'JUDICIAL')
        .eq('numero_processo', painel.numero_processo).not('advogados', 'is', null).limit(1)
        .then(({ data }) => setPainelAdv((data?.[0]?.advogados as any)?.[0] ?? null))
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [painelId])

  return (
    <div className="flex min-h-screen lg:h-screen">
      <div className="flex-1 min-w-0 flex flex-col">

        {/* ===== Cabeçalho + KPIs ===== */}
        <div className="px-6 py-4 border-b border-border bg-bg-secondary">
          <div className="flex items-center justify-between flex-wrap gap-2">
            <div className="flex items-center gap-2">
              <Radar className="w-4 h-4 text-vf-red-light" />
              <h1 className="text-text-primary font-semibold">Central de Oportunidades</h1>
              <span className="text-text-faint text-xs">alerta → validação humana → oportunidade</span>
            </div>
            <span className="text-text-faint text-[11px]">
              {atualizadoEm ? <>atualizado às {atualizadoEm.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' })}</> : '…'} · fonte: alertas + DJEN (18/08) · coleta recorrente não ligada
            </span>
          </div>

          <div className="flex gap-2 mt-3 flex-wrap text-xs">
            {[
              { n: kpi.candidatos, l: 'candidatos', cls: 'text-info' },
              { n: kpi.aguardando, l: 'aguard. validação', cls: 'text-warning' },
              { n: kpi.validados, l: 'validados', cls: 'text-success' },
              { n: kpi.conflitos, l: 'conflitos', cls: 'text-danger' },
              { n: kpi.criticos, l: 'críticos', cls: 'text-danger' },
            ].map(k => (
              <div key={k.l} className="px-3 py-1.5 rounded bg-bg-primary border border-border">
                <b className={cn('tabular-nums', k.cls)}>{k.n}</b> <span className="text-text-muted">{k.l}</span>
              </div>
            ))}
            <div className="px-3 py-1.5 rounded bg-bg-primary border border-border">
              <b className="text-text-primary tabular-nums">{formatBRLCompact(kpi.divida)}</b>{' '}
              <span className="text-text-muted">dívida PGFN dos ativos</span>{' '}
              <span className="text-text-faint">(não é valor de proposta)</span>
            </div>
          </div>
        </div>

        <div className="flex-1 overflow-y-auto p-6 space-y-6">

          {/* ===== Prioridades (regra explicada, nunca "mágica") ===== */}
          <section>
            <p className="section-header">Prioridades — crítico &gt; corroborado &gt; recente · credora/conflito nunca entram</p>
            {loading ? <p className="text-text-muted text-sm">Carregando…</p> : (
              <div className="grid sm:grid-cols-2 xl:grid-cols-4 gap-3">
                {prioridades.map(a => {
                  const f = fila.get(a.cnpj_raiz)
                  const dias = diasDe(a.detectado_em)
                  return (
                    <button key={a.id} onClick={() => setPainelId(a.id)}
                      className={cn('card !p-3.5 text-left hover:border-border-strong transition-colors',
                        a.gravidade === 'CRITICO' && 'border-danger/40')}>
                      <div className="flex items-start justify-between gap-2">
                        <p className="font-semibold text-sm text-text-primary leading-snug line-clamp-2">
                          {f?.nome_devedor ?? a.titulo}
                        </p>
                        {a.gravidade === 'CRITICO' && <span className="badge bg-danger/15 text-danger flex-shrink-0">crítico</span>}
                      </div>
                      <p className="text-xs text-text-muted mt-1.5 line-clamp-2">{a.titulo.replace(/^Pauta 20\/08 · /, '')}</p>
                      <div className="flex items-center gap-2 mt-2 text-[11px] flex-wrap">
                        <span className={cn('font-medium', EVID_META[a.evidencia_condicao])}>{a.evidencia_condicao}</span>
                        <span className="text-text-faint">· {dias === 0 ? 'hoje' : `${dias}d`}</span>
                        {f && <span className="text-text-primary font-semibold tabular-nums">· {formatBRLCompact(f.valor_total_devida)} PGFN</span>}
                        {a.responsavel && <span className="text-text-faint">· {a.responsavel}</span>}
                      </div>
                      <p className="text-[11px] text-vf-red-light mt-2">→ {proximaAcao(a)}</p>
                      {a.pendencias.length > 0 && (
                        <p className="text-[10px] text-warning mt-1">falta: {a.pendencias.slice(0, 2).map(p => PENDENCIA_LABELS[p] ?? p).join(' · ')}{a.pendencias.length > 2 ? '…' : ''}</p>
                      )}
                    </button>
                  )
                })}
              </div>
            )}
          </section>

          {/* ===== Filtros + tabela compacta ===== */}
          <section>
            <div className="flex flex-wrap gap-2 mb-3">
              <div className="relative">
                <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-text-faint" />
                <input className="input pl-8 py-1.5 text-xs w-56" placeholder="Buscar empresa…"
                  value={busca} onChange={e => setBusca(e.target.value)} />
              </div>
              <select className="select py-1.5 text-xs w-auto" value={fEstado} onChange={e => setParam({ estado: e.target.value })}>
                <option value="">Estado</option>
                {Object.entries(ESTADO_META).map(([k, v]) => <option key={k} value={k}>{v.label}</option>)}
              </select>
              <select className="select py-1.5 text-xs w-auto" value={fGrav} onChange={e => setParam({ grav: e.target.value })}>
                <option value="">Gravidade</option>
                <option value="CRITICO">Crítico</option><option value="IMPORTANTE">Importante</option><option value="INFORMATIVO">Informativo</option>
              </select>
              <select className="select py-1.5 text-xs w-auto" value={fPapel} onChange={e => setParam({ papel: e.target.value })}>
                <option value="">Papel</option>
                {Object.entries(PAPEL_META).map(([k, v]) => <option key={k} value={k}>{v.label}</option>)}
              </select>
              <select className="select py-1.5 text-xs w-auto" value={fRamo} onChange={e => setParam({ ramo: e.target.value })}>
                <option value="">Ramo</option>
                <option value="FEDERAL">Federal</option><option value="TRABALHISTA">Trabalhista</option>
                <option value="ESTADUAL">Estadual</option><option value="OUTRO">Outro</option>
              </select>
              <select className="select py-1.5 text-xs w-auto" value={fResp} onChange={e => setParam({ resp: e.target.value })}>
                <option value="">Responsável</option>
                <option value="RODRIGO">Rodrigo</option><option value="ANA">Ana</option>
              </select>
              <span className="text-text-faint text-xs self-center">{filtrados.length} de {alertas.length}</span>
            </div>

            <div className="tblwrap overflow-x-auto border border-border rounded-lg">
              <table className="table-vf min-w-[900px]">
                <thead><tr>
                  <th>Empresa</th><th>Evento / gatilho</th><th>Papel</th><th>Ramo</th>
                  <th>Detectado</th>
                  <th className="text-right">Dívida PGFN</th>
                  <th className="text-right">Valor proc.</th>
                  <th>Evidência</th><th>Estado</th><th>Resp.</th><th>Próxima ação</th>
                </tr></thead>
                <tbody>
                  {filtrados.map(a => {
                    const f = fila.get(a.cnpj_raiz)
                    const meta = a.cnpj_raiz === '00000000'
                    const dias = diasDe(a.detectado_em)
                    return (
                      <tr key={a.id} onClick={() => setPainelId(a.id)}
                        className={cn('cursor-pointer', painelId === a.id && 'bg-bg-hover')}>
                        <td className="max-w-[180px]">
                          <span className="text-text-primary text-xs font-medium block truncate">
                            {meta ? '⚙ Auditoria' : (f?.nome_devedor ?? a.cnpj_raiz)}
                          </span>
                        </td>
                        <td className="max-w-[220px]">
                          <span className="text-xs text-text-muted block truncate" title={a.titulo}>
                            {a.padrao_playbook ? `${a.padrao_playbook.slice(0,2)} · ` : ''}{a.titulo.replace(/^Pauta 20\/08 · /, '')}
                          </span>
                        </td>
                        <td><span className={cn('text-xs', PAPEL_META[a.papel_processual]?.cls)}>{PAPEL_META[a.papel_processual]?.label ?? a.papel_processual}</span></td>
                        <td className="text-xs text-text-muted">{a.ramo?.toLowerCase() ?? '—'}</td>
                        <td className="text-xs text-text-muted whitespace-nowrap">{formatDate(a.detectado_em)} <span className="text-text-faint">({dias === 0 ? 'hoje' : `${dias}d`})</span></td>
                        <td className="text-right text-xs tabular-nums text-text-primary">{meta ? '—' : formatBRLCompact(f?.valor_total_devida)}</td>
                        <td className="text-right text-xs tabular-nums text-text-faint">{a.valor_processo ? formatBRLCompact(a.valor_processo) : 'não extraído'}</td>
                        <td><span className={cn('text-[11px] font-medium', EVID_META[a.evidencia_condicao])}>{a.evidencia_condicao}</span></td>
                        <td><span className={cn('badge text-[10px]', ESTADO_META[a.estado].cls)}>{ESTADO_META[a.estado].label}</span></td>
                        <td className="text-xs text-text-muted">{a.responsavel ?? '—'}</td>
                        <td className="text-[11px] text-vf-red-light whitespace-nowrap">{proximaAcao(a)}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          </section>
        </div>
      </div>

      {/* ===== Painel lateral de validação — overlay no mobile, lateral no desktop ===== */}
      {painel && (
        <aside className="fixed inset-0 z-40 lg:static lg:inset-auto lg:z-auto w-full lg:w-[380px] flex-shrink-0 border-l border-border bg-bg-secondary overflow-y-auto">
          <div className="p-5 space-y-4">
            <div className="flex items-start justify-between gap-2">
              <div>
                <p className="font-semibold text-text-primary leading-snug">
                  {painelFila?.nome_devedor ?? (painel.cnpj_raiz === '00000000' ? 'Alerta de auditoria' : painel.cnpj_raiz)}
                </p>
                {painel.cnpj_raiz !== '00000000' && (
                  <Link href={`/empresa/${painel.cnpj_raiz}`} className="text-xs text-info hover:underline">
                    Abrir ficha completa <ArrowUpRight className="w-3 h-3 inline" />
                  </Link>
                )}
              </div>
              <button onClick={() => setPainelId(null)} className="btn-ghost !p-1"><X className="w-4 h-4" /></button>
            </div>

            <div className="flex gap-1.5 flex-wrap">
              <span className={cn('badge text-[10px]', ESTADO_META[painel.estado].cls)}>{ESTADO_META[painel.estado].label}</span>
              <span className={cn('badge text-[10px] bg-bg-primary', EVID_META[painel.evidencia_condicao])}>{painel.evidencia_condicao}</span>
              <span className={cn('badge text-[10px] bg-bg-primary', PAPEL_META[painel.papel_processual]?.cls)}>{PAPEL_META[painel.papel_processual]?.label}</span>
              {painel.gravidade === 'CRITICO' && <span className="badge bg-danger/15 text-danger text-[10px]">crítico</span>}
            </div>

            <div className="card !p-3.5 space-y-1.5 text-xs">
              <p className="text-text-primary font-medium">{painel.titulo}</p>
              {painel.numero_processo && <p className="text-text-muted font-mono text-[11px]">{painel.numero_processo}</p>}
              <p className="text-text-muted">Detectado em {formatDate(painel.detectado_em)} · fonte: <span className="text-text-primary">{painel.fonte ?? 'NÃO LOCALIZADA'}</span></p>
              {painelFila && <p className="text-text-muted">Dívida PGFN: <b className="text-text-primary">{formatBRLCompact(painelFila.valor_total_devida)}</b> · Valor do processo: <b className="text-text-primary">{painel.valor_processo ? formatBRLCompact(painel.valor_processo) : 'não extraído'}</b></p>}
              {painel.score_legado != null && (
                <p className="text-text-faint">Score legado (histórico, não autoritativo): {Math.round(Number(painel.score_legado))}</p>
              )}
            </div>

            {painel.pendencias.length > 0 && (
              <div className="card !p-3.5">
                <p className="section-header !mb-2 flex items-center gap-1.5"><ShieldQuestion className="w-3.5 h-3.5 text-warning" /> O que falta confirmar</p>
                <ul className="space-y-1">
                  {painel.pendencias.map(p => (
                    <li key={p} className="text-xs text-warning flex items-start gap-1.5">
                      <AlertTriangle className="w-3 h-3 mt-0.5 flex-shrink-0" />{PENDENCIA_LABELS[p] ?? p}
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {(painelAdv || painelContatos.length > 0) && (
              <div className="card !p-3.5 space-y-1.5">
                <p className="section-header !mb-1">Abordagem — sem caça a contato</p>
                {painelAdv && (
                  <p className="text-xs text-text-primary flex items-center gap-1.5">
                    <Scale className="w-3 h-3 text-vf-red-light flex-shrink-0" />
                    {painelAdv.nome}{painelAdv.oab ? ` · OAB ${painelAdv.oab}${painelAdv.uf ? '/' + painelAdv.uf : ''}` : ''}
                    <span className="text-text-faint">(advogado dos autos — interlocutor potencial, não decisor)</span>
                  </p>
                )}
                {painelContatos.filter(c => c.telefone).map((c, i) => (
                  <p key={i} className="text-xs text-text-muted">
                    ☎ <span className="font-mono text-text-primary">{c.telefone}</span>{' '}
                    <span className="text-text-faint">({c.cargo === 'CADASTRO RFB' ? 'cadastro Receita' : c.cargo ?? 'contato'})</span>
                  </p>
                ))}
                {painelContatos.filter(c => c.email).map((c, i) => (
                  <p key={i} className="text-xs"><a className="text-info hover:underline" href={`mailto:${c.email}`}>{c.email}</a></p>
                ))}
              </div>
            )}

            {painel.trecho && (
              <div className="card !p-3.5">
                <p className="section-header !mb-2 flex items-center gap-1.5"><Scale className="w-3.5 h-3.5 text-vf-red-light" /> Trecho da fonte</p>
                <p className="text-xs text-text-muted leading-relaxed">{painel.trecho}</p>
                {painel.link_fonte && (
                  <a href={painel.link_fonte} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1 text-xs text-info hover:underline mt-2">
                    <ExternalLink className="w-3 h-3" /> Abrir publicação original
                  </a>
                )}
              </div>
            )}

            {/* Ações de validação — toda decisão grava autoria e trilha */}
            <div className="card !p-3.5 space-y-2.5">
              <p className="section-header !mb-1">Decisão (grava autoria e trilha)</p>
              <textarea className="input text-xs min-h-[56px]" placeholder="Nota da validação (recomendado; obrigatório p/ conflito e descarte)"
                value={nota} onChange={e => setNota(e.target.value)} />
              <div className="grid grid-cols-2 gap-2">
                <button disabled={salvando} onClick={() => mudarEstado(painel, 'VALIDADO')}
                  className="btn-primary !py-1.5 text-xs justify-center"><CheckCircle2 className="w-3.5 h-3.5" /> Validar</button>
                <button disabled={salvando || !nota.trim()} onClick={() => mudarEstado(painel, 'CONFLITO')}
                  className="btn-secondary !py-1.5 text-xs justify-center"><AlertTriangle className="w-3.5 h-3.5" /> Conflito</button>
                <button disabled={salvando} onClick={() => mudarEstado(painel, 'MONITORAR')}
                  className="btn-secondary !py-1.5 text-xs justify-center"><Eye className="w-3.5 h-3.5" /> Monitorar</button>
                <button disabled={salvando || !nota.trim()} onClick={() => mudarEstado(painel, 'DESCARTADO')}
                  className="btn-secondary !py-1.5 text-xs justify-center text-danger"><XCircle className="w-3.5 h-3.5" /> Descartar</button>
              </div>
              <button disabled={salvando || painel.estado !== 'VALIDADO'} onClick={() => promover(painel)}
                title={painel.estado !== 'VALIDADO' ? 'O banco recusa promoção sem VALIDADO (fn_promover_alerta)' : ''}
                className={cn('w-full !py-2 text-xs justify-center inline-flex items-center gap-2 rounded font-medium transition-colors',
                  painel.estado === 'VALIDADO' ? 'bg-success/20 text-success hover:bg-success/30' : 'bg-bg-hover text-text-faint cursor-not-allowed')}>
                <ArrowUpRight className="w-3.5 h-3.5" /> Promover a oportunidade
              </button>
              <p className="text-[10px] text-text-faint flex items-center gap-1"><Clock className="w-3 h-3" /> Promoção exige estado VALIDADO — regra imposta pelo banco, não pela tela.</p>
            </div>
          </div>
        </aside>
      )}
    </div>
  )
}
