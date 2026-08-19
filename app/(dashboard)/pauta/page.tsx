'use client'

/**
 * PAUTA — a linha de trabalho do dia.
 * Máx. N itens por sócio, selecionados pelo playbook (P0–P5) sobre os teores do DJEN.
 * Tudo que a abordagem precisa numa tela: gatilho, advogado dos autos, telefone/e-mail
 * cadastral (Receita), sócios, trecho da decisão e link da publicação.
 */

import { useCallback, useEffect, useState } from 'react'
import Link from 'next/link'
import { createClient } from '@/lib/supabase/client'
import { formatBRLCompact, formatCnpj, formatDate, cn } from '@/lib/utils'
import { ScoreBadge, ZonaBadge } from '@/components/intel'
import type { FilaRow } from '@/lib/types'
import {
  CalendarCheck, Phone, Mail, Scale, Users, ExternalLink,
  CheckCircle2, Circle, AlertTriangle, Sparkles,
} from 'lucide-react'

interface PautaItem {
  id: string
  data: string
  cnpj_raiz: string
  responsavel: 'RODRIGO' | 'ANA'
  padrao: string
  numero_processo: string | null
  motivo: string | null
  alerta: string | null
  concluido: boolean
}
interface EventoInfo {
  numero_processo: string
  advogados: { nome: string; oab: string | null; uf: string | null }[] | null
  texto: string | null
  link_publicacao: string | null
}
interface ContatoInfo { cnpj_raiz: string; telefone: string | null; email: string | null; cargo: string | null }
interface EmpresaInfo { cnpj_raiz: string; socios: { nome?: string }[] | null }

const PADRAO_LABELS: Record<string, string> = {
  P0_CONSTRICAO_FRESCA: 'Constrição fresca',
  P1_EMBARGOS:          'Embargos sem garantia',
  P2_GARANTIA_EXIGIDA:  'Garantia exigida',
  P3_PENHORA_IMOVEL:    'Substituição de penhora',
  P4_APOLICE:           'Renovação de apólice',
  P5_NPJ:               'NPJ descumprido',
}

export default function PautaPage() {
  const supabase = createClient()
  const [itens, setItens] = useState<PautaItem[]>([])
  const [fila, setFila] = useState<Map<string, FilaRow>>(new Map())
  const [eventos, setEventos] = useState<Map<string, EventoInfo>>(new Map())
  const [contatos, setContatos] = useState<Map<string, ContatoInfo[]>>(new Map())
  const [socios, setSocios] = useState<Map<string, string[]>>(new Map())
  const [dataPauta, setDataPauta] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  const fetchAll = useCallback(async () => {
    setLoading(true)
    // pauta mais recente disponível
    const { data: ult } = await supabase.from('pauta')
      .select('data').order('data', { ascending: false }).limit(1)
    const dia = ult?.[0]?.data ?? null
    setDataPauta(dia)
    if (!dia) { setItens([]); setLoading(false); return }

    const { data: rows } = await supabase.from('pauta')
      .select('*').eq('data', dia).order('responsavel').order('criado_em')
    const lista = (rows ?? []) as PautaItem[]
    setItens(lista)

    const cnpjs = [...new Set(lista.map(i => i.cnpj_raiz))]
    const procs = [...new Set(lista.map(i => i.numero_processo).filter(Boolean))] as string[]
    const [f, ev, ct, em] = await Promise.all([
      supabase.from('vw_fila_oportunidades').select('*').in('cnpj_raiz', cnpjs),
      supabase.from('eventos')
        .select('numero_processo,advogados,texto,link_publicacao')
        .in('numero_processo', procs).eq('fonte', 'JUDICIAL').not('texto', 'is', null),
      supabase.from('contatos').select('cnpj_raiz,telefone,email,cargo').in('cnpj_raiz', cnpjs),
      supabase.from('empresas').select('cnpj_raiz,socios').in('cnpj_raiz', cnpjs),
    ])
    setFila(new Map(((f.data ?? []) as FilaRow[]).map(r => [r.cnpj_raiz, r])))
    const evMap = new Map<string, EventoInfo>()
    for (const e of (ev.data ?? []) as EventoInfo[]) if (!evMap.has(e.numero_processo)) evMap.set(e.numero_processo, e)
    setEventos(evMap)
    const ctMap = new Map<string, ContatoInfo[]>()
    for (const c of (ct.data ?? []) as ContatoInfo[]) {
      const arr = ctMap.get(c.cnpj_raiz) ?? []; arr.push(c); ctMap.set(c.cnpj_raiz, arr)
    }
    setContatos(ctMap)
    const soMap = new Map<string, string[]>()
    for (const e of (em.data ?? []) as EmpresaInfo[])
      soMap.set(e.cnpj_raiz, (e.socios ?? []).map(s => s?.nome).filter(Boolean).slice(0, 2) as string[])
    setSocios(soMap)
    setLoading(false)
  }, [supabase])

  useEffect(() => { fetchAll() }, [fetchAll])

  async function toggleConcluido(item: PautaItem) {
    setItens(prev => prev.map(i => i.id === item.id ? { ...i, concluido: !i.concluido } : i))
    await supabase.from('pauta').update({ concluido: !item.concluido }).eq('id', item.id)
  }

  const porResponsavel = (r: 'RODRIGO' | 'ANA') => itens.filter(i => i.responsavel === r)
  const feitos = itens.filter(i => i.concluido).length

  return (
    <div className="p-6 max-w-6xl mx-auto">
      <div className="flex items-end justify-between gap-4 flex-wrap mb-1">
        <div>
          <h1 className="text-xl font-bold text-text-primary flex items-center gap-2">
            <CalendarCheck className="w-5 h-5 text-vf-red" />
            Pauta do dia
          </h1>
          <p className="text-text-muted text-sm mt-1">
            {dataPauta ? <>Linha de trabalho de <span className="text-text-primary font-medium">{formatDate(dataPauta)}</span> · selecionada pelo playbook sobre os teores do DJEN</> : 'Nenhuma pauta gerada ainda.'}
          </p>
        </div>
        {itens.length > 0 && (
          <div className="text-right">
            <p className="text-2xl font-bold text-text-primary tabular-nums">{feitos}<span className="text-text-faint">/{itens.length}</span></p>
            <p className="text-xs text-text-muted">abordagens concluídas</p>
          </div>
        )}
      </div>

      {loading ? (
        <p className="text-text-muted text-sm mt-10">Carregando pauta…</p>
      ) : itens.length === 0 ? (
        <div className="card mt-8 text-center py-12">
          <p className="text-text-primary font-medium">Sem pauta para hoje</p>
          <p className="text-text-muted text-sm mt-1">A pauta é gerada pela seleção do playbook. Enquanto isso, a <Link className="text-info hover:underline" href="/oportunidades">fila completa</Link> continua disponível.</p>
        </div>
      ) : (
        <div className="grid lg:grid-cols-2 gap-6 mt-6">
          {(['RODRIGO', 'ANA'] as const).map(resp => (
            <section key={resp}>
              <h2 className="section-header">{resp === 'RODRIGO' ? 'Rodrigo' : 'Ana'} · {porResponsavel(resp).length} itens</h2>
              <div className="space-y-3">
                {porResponsavel(resp).map(item => {
                  const f = fila.get(item.cnpj_raiz)
                  const ev = item.numero_processo ? eventos.get(item.numero_processo) : undefined
                  const adv = ev?.advogados?.[0]
                  const cts = contatos.get(item.cnpj_raiz) ?? []
                  const soc = socios.get(item.cnpj_raiz) ?? []
                  const destaque = item.motivo?.startsWith('★')
                  return (
                    <div key={item.id} className={cn('card !p-4 transition-opacity', item.concluido && 'opacity-50', destaque && 'border-vf-red')}>
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <Link href={`/empresa/${item.cnpj_raiz}`} className="font-semibold text-text-primary hover:text-vf-red-light transition-colors leading-snug block truncate">
                            {f?.nome_devedor ?? formatCnpj(item.cnpj_raiz)}
                          </Link>
                          <div className="flex items-center gap-2 mt-1 flex-wrap">
                            {destaque && <span className="badge bg-vf-red/15 text-vf-red-light"><Sparkles className="w-3 h-3 mr-1" />destaque</span>}
                            <span className="badge bg-vf-red/10 text-vf-red-light">{PADRAO_LABELS[item.padrao] ?? item.padrao}</span>
                            {f?.zona_risco && <ZonaBadge zona={f.zona_risco} />}
                            {f && <ScoreBadge score={f.score} />}
                          </div>
                        </div>
                        <button onClick={() => toggleConcluido(item)} title={item.concluido ? 'Reabrir' : 'Marcar concluída'}
                          className="flex-shrink-0 text-text-faint hover:text-success transition-colors">
                          {item.concluido ? <CheckCircle2 className="w-6 h-6 text-success" /> : <Circle className="w-6 h-6" />}
                        </button>
                      </div>

                      <p className="text-sm text-text-muted mt-2">{item.motivo?.replace(/^★ /, '')}</p>
                      {item.alerta && (
                        <p className="text-xs text-warning mt-1.5 flex items-start gap-1.5">
                          <AlertTriangle className="w-3.5 h-3.5 flex-shrink-0 mt-0.5" />{item.alerta}
                        </p>
                      )}

                      <div className="grid sm:grid-cols-2 gap-x-4 gap-y-1.5 mt-3 text-xs">
                        {f && (
                          <p className="text-text-muted">Dívida <b className="text-text-primary font-semibold">{formatBRLCompact(f.valor_total_devida)}</b>
                            {f.uf_devedor && <span className="text-text-faint"> · {f.uf_devedor}</span>}</p>
                        )}
                        {adv && (
                          <p className="text-text-muted flex items-center gap-1.5 truncate">
                            <Scale className="w-3 h-3 flex-shrink-0 text-vf-red-light" />
                            <span className="truncate">{adv.nome}{adv.oab ? ` · OAB ${adv.oab}${adv.uf ? '/' + adv.uf : ''}` : ''}</span>
                          </p>
                        )}
                        {cts.filter(c => c.telefone).slice(0, 1).map((c, i) => (
                          <p key={i} className="text-text-muted flex items-center gap-1.5">
                            <Phone className="w-3 h-3 flex-shrink-0 text-success" />
                            <span className="font-mono text-text-primary">{c.telefone}</span>
                            <span className="text-text-faint">({c.cargo === 'CADASTRO RFB' ? 'Receita' : c.cargo ?? 'contato'})</span>
                          </p>
                        ))}
                        {cts.filter(c => c.email).slice(0, 1).map((c, i) => (
                          <p key={i} className="text-text-muted flex items-center gap-1.5 truncate">
                            <Mail className="w-3 h-3 flex-shrink-0 text-info" />
                            <a href={`mailto:${c.email}`} className="text-info hover:underline truncate">{c.email}</a>
                          </p>
                        ))}
                        {soc.length > 0 && (
                          <p className="text-text-muted flex items-center gap-1.5 truncate sm:col-span-2">
                            <Users className="w-3 h-3 flex-shrink-0" />
                            <span className="truncate">{soc.join(' · ')}</span>
                          </p>
                        )}
                      </div>

                      {ev?.texto && (
                        <details className="mt-3">
                          <summary className="text-xs text-text-faint cursor-pointer hover:text-text-muted select-none">
                            Trecho da publicação {item.numero_processo && <span className="font-mono">· {item.numero_processo}</span>}
                          </summary>
                          <p className="text-xs text-text-muted mt-2 bg-bg-secondary rounded p-3 leading-relaxed">
                            {ev.texto.slice(0, 600)}{ev.texto.length > 600 ? '…' : ''}
                          </p>
                          {ev.link_publicacao && (
                            <a href={ev.link_publicacao} target="_blank" rel="noreferrer"
                              className="inline-flex items-center gap-1 text-xs text-info hover:underline mt-1.5">
                              <ExternalLink className="w-3 h-3" /> Publicação no DJEN
                            </a>
                          )}
                        </details>
                      )}
                    </div>
                  )
                })}
              </div>
            </section>
          ))}
        </div>
      )}

      {itens.length > 0 && (
        <p className="text-xs text-text-faint mt-8 border-t border-border pt-4 max-w-3xl">
          Telefones/e-mails marcados “Receita” vêm do cadastro público do CNPJ. Ressalva desta safra: o re-teste de
          recorrência contra a base PGFN completa ainda não rodou — jurídico tributário estruturado na conversa = descartar e registrar.
        </p>
      )}
    </div>
  )
}
