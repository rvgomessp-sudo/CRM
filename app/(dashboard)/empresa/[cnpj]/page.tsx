'use client'

import { useEffect, useState } from 'react'
import { useParams, useRouter } from 'next/navigation'
import Link from 'next/link'
import { createClient } from '@/lib/supabase/client'
import {
  formatBRL, formatBRLCompact, formatDate, formatDateTime,
  formatCnpj, cn
} from '@/lib/utils'
import {
  STAGE_LABELS, STAGES_ORDERED, MOTOR_COLORS, MOTOR_BADGE_LABEL,
  type Empresa, type Inscricao, type Interacao, type ConsultaSeguradora,
  type Proposta, type PipelineStage, type MotorTipo, type CanalInteracao
} from '@/lib/types'
import { MOTOR_ABORDAGEM } from '@/lib/motor'
import {
  ArrowLeft, Building2, FileText, MessageSquare, Calculator,
  AlertTriangle, CheckCircle, Clock, Plus, ExternalLink, ChevronDown
} from 'lucide-react'

type Tab = 'overview' | 'inscricoes' | 'interacoes' | 'proposta'

export default function EmpresaPage() {
  const params = useParams()
  const cnpj = params.cnpj as string
  const supabase = createClient()

  const [empresa, setEmpresa] = useState<Empresa | null>(null)
  const [inscricoes, setInscricoes] = useState<Inscricao[]>([])
  const [interacoes, setInteracoes] = useState<Interacao[]>([])
  const [consultas, setConsultas] = useState<ConsultaSeguradora[]>([])
  const [propostas, setPropostas] = useState<Proposta[]>([])
  const [loading, setLoading] = useState(true)
  const [tab, setTab] = useState<Tab>('overview')
  const [saving, setSaving] = useState(false)
  const [showInteracaoForm, setShowInteracaoForm] = useState(false)

  // Form nova interação
  const [novaInteracao, setNovaInteracao] = useState({
    canal: 'TELEFONE' as CanalInteracao,
    resumo: '',
    proxima_acao: '',
    proxima_acao_em: ''
  })

  useEffect(() => {
    fetchAll()
  }, [cnpj])

  async function fetchAll() {
    setLoading(true)
    const [emp, ins, int, con, prop] = await Promise.all([
      supabase.from('empresas').select('*, responsavel:profiles(id, nome)').eq('cnpj_raiz', cnpj).single(),
      supabase.from('inscricoes').select('*').eq('cnpj_raiz', cnpj).order('valor_numerico', { ascending: false }),
      supabase.from('interacoes').select('*, responsavel:profiles(id, nome)').eq('cnpj_raiz', cnpj).order('criado_em', { ascending: false }),
      supabase.from('consultas_seguradora').select('*').eq('cnpj_raiz', cnpj).order('data_consulta', { ascending: false }),
      supabase.from('propostas').select('*').eq('cnpj_raiz', cnpj).order('criado_em', { ascending: false }),
    ])
    setEmpresa(emp.data)
    setInscricoes(ins.data || [])
    setInteracoes(int.data || [])
    setConsultas(con.data || [])
    setPropostas(prop.data || [])
    setLoading(false)
  }

  async function updateEstagio(novoEstagio: PipelineStage) {
    if (!empresa) return
    setSaving(true)
    await supabase.from('empresas').update({ estagio: novoEstagio, atualizado_em: new Date().toISOString() }).eq('cnpj_raiz', cnpj)
    setEmpresa(e => e ? { ...e, estagio: novoEstagio } : null)
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
    // Atualiza último contato
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

  if (loading) {
    return <div className="flex items-center justify-center h-screen text-text-muted">Carregando ficha…</div>
  }
  if (!empresa) {
    return <div className="p-8 text-danger">Empresa não encontrada: {cnpj}</div>
  }

  const motor = empresa.motor as MotorTipo | null

  return (
    <div className="flex flex-col min-h-screen">

      {/* Header */}
      <div className="px-6 py-4 border-b border-border bg-bg-secondary">
        <div className="flex items-center gap-3 mb-3">
          <Link href="/base-pgfn" className="btn-ghost py-1 text-xs">
            <ArrowLeft className="w-3.5 h-3.5" /> Voltar
          </Link>
          {motor && (
            <span className={cn('badge', MOTOR_COLORS[motor])}>
              {MOTOR_BADGE_LABEL[motor]}
            </span>
          )}
          {empresa.prioridade === 'ALTA' && (
            <span className="badge bg-red-500/20 text-red-400">
              <AlertTriangle className="w-3 h-3 mr-1" /> ALTA
            </span>
          )}
        </div>

        <div className="flex items-start justify-between gap-4">
          <div>
            <h1 className="text-xl font-bold text-text-primary">{empresa.nome_devedor}</h1>
            <p className="text-text-muted text-sm">{formatCnpj(empresa.cnpj_completo)} | UF: {empresa.uf_devedor}</p>
          </div>

          <div className="text-right">
            <p className="text-2xl font-bold text-vf-red-light">{formatBRLCompact(empresa.valor_total_devida)}</p>
            <p className="text-text-muted text-xs">{empresa.qtd_inscricoes} inscrições PGFN</p>
          </div>
        </div>

        {/* Estágio selector */}
        <div className="flex items-center gap-3 mt-3">
          <span className="text-text-faint text-xs">Estágio:</span>
          <div className="relative">
            <select
              className="select py-1 text-xs pr-8 appearance-none"
              value={empresa.estagio}
              onChange={e => updateEstagio(e.target.value as PipelineStage)}
              disabled={saving}
            >
              {STAGES_ORDERED.map(s => (
                <option key={s} value={s}>{STAGE_LABELS[s]}</option>
              ))}
            </select>
            <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 w-3 h-3 text-text-faint pointer-events-none" />
          </div>
          <span className="text-text-muted text-xs">→ {empresa.seguradora_alvo}</span>
          {saving && <span className="text-text-faint text-xs">Salvando…</span>}
        </div>

        {/* Abordagem do motor */}
        {motor && (
          <div className="mt-3 p-2.5 rounded bg-bg-card border border-border text-xs text-text-muted">
            <span className="font-semibold text-text-primary">Script {motor}:</span> {MOTOR_ABORDAGEM[motor]}
          </div>
        )}
      </div>

      {/* Tabs */}
      <div className="border-b border-border bg-bg-secondary px-6">
        <div className="flex gap-0">
          {([
            { id: 'overview',    label: 'Overview',    icon: Building2 },
            { id: 'inscricoes',  label: `Inscrições (${inscricoes.length})`, icon: FileText },
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

      {/* Tab content */}
      <div className="flex-1 p-6 overflow-auto">

        {/* OVERVIEW */}
        {tab === 'overview' && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 max-w-4xl">

            {/* Dados PGFN */}
            <div className="card">
              <p className="section-header">Dados PGFN</p>
              <dl className="space-y-2 text-sm">
                <Row label="Regime" value={empresa.regime_tributario || '—'} />
                <Row label="Capital Social" value={empresa.capital_social ? formatBRL(empresa.capital_social) : '—'} />
                <Row label="PL Estimado" value={empresa.pl_estimado ? formatBRL(empresa.pl_estimado) : '—'} />
                <Row label="Receita Estimada" value={empresa.receita_estimada ? formatBRLCompact(empresa.receita_estimada) : '—'} />
                <Row label="Situação CNPJ" value={empresa.cnpj_situacao || '—'} />
                <Row label="NDA" value={empresa.nda_assinado ? `✓ Assinado em ${formatDate(empresa.nda_data)}` : 'Não assinado'} />
              </dl>
            </div>

            {/* Consulta Seguradora */}
            <div className="card">
              <div className="flex items-center justify-between mb-3">
                <p className="section-header mb-0">Consulta Seguradora</p>
                <span className="text-text-muted text-xs">{empresa.seguradora_alvo}</span>
              </div>
              {consultas.length === 0 ? (
                <p className="text-text-faint text-sm">Nenhuma consulta registrada.</p>
              ) : (
                <dl className="space-y-2 text-sm">
                  {consultas.slice(0,1).map(c => (
                    <div key={c.id}>
                      <Row label="Status" value={c.status} />
                      <Row label="Limite" value={c.limite_aprovado ? formatBRL(c.limite_aprovado) : '—'} />
                      <Row label="Taxa" value={c.taxa_indicativa ? `${(c.taxa_indicativa * 100).toFixed(2)}% a.a.` : '—'} />
                      <Row label="Modalidade" value={c.modalidade || '—'} />
                      <Row label="Validade" value={formatDate(c.validade_ate)} />
                    </div>
                  ))}
                </dl>
              )}
            </div>

            {/* Decisor */}
            <div className="card">
              <p className="section-header">Decisor</p>
              <dl className="space-y-2 text-sm">
                <Row label="Nome" value={empresa.decisor_nome || '—'} />
                <Row label="Cargo" value={empresa.decisor_cargo || '—'} />
                <Row label="E-mail" value={empresa.decisor_email || '—'} />
                <Row label="Telefone" value={empresa.decisor_telefone || '—'} />
                {empresa.decisor_linkedin && (
                  <div className="flex justify-between">
                    <span className="text-text-faint">LinkedIn</span>
                    <a href={empresa.decisor_linkedin} target="_blank" rel="noreferrer"
                       className="text-info flex items-center gap-1">
                      Perfil <ExternalLink className="w-3 h-3" />
                    </a>
                  </div>
                )}
              </dl>
            </div>

            {/* SLA */}
            <div className="card">
              <p className="section-header">SLA / Próxima Ação</p>
              <dl className="space-y-2 text-sm">
                <Row label="Último contato" value={formatDateTime(empresa.ultimo_contato_em)} />
                <Row label="Última atualização" value={formatDateTime(empresa.atualizado_em)} />
                <Row label="Próxima ação" value={empresa.proxima_acao_descricao || '—'} />
                <Row label="Data" value={formatDate(empresa.proxima_acao_em)} />
                {empresa.notas && (
                  <div>
                    <span className="text-text-faint block mb-1">Notas</span>
                    <p className="text-text-primary text-xs">{empresa.notas}</p>
                  </div>
                )}
              </dl>
            </div>
          </div>
        )}

        {/* INSCRIÇÕES */}
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
                    <th>Nº Inscrição</th>
                    <th>Tributo</th>
                    <th>Situação</th>
                    <th>Motor</th>
                    <th>Data</th>
                    <th>Dias</th>
                    <th className="text-right">Valor</th>
                    <th>Garantia</th>
                    <th>Ajuizado</th>
                  </tr>
                </thead>
                <tbody>
                  {inscricoes.map(ins => (
                    <tr key={ins.id}>
                      <td className="font-mono text-xs text-text-muted">{ins.numero_inscricao}</td>
                      <td>
                        <span className="badge bg-bg-secondary text-text-muted text-[10px]">
                          {ins.tributo || '—'}
                        </span>
                      </td>
                      <td className="text-xs text-text-muted max-w-36 truncate">{ins.situacao_inscricao}</td>
                      <td>
                        {ins.motor && (
                          <span className={cn('badge text-[10px]', MOTOR_COLORS[ins.motor as MotorTipo])}>
                            {ins.motor}
                          </span>
                        )}
                      </td>
                      <td className="text-xs text-text-muted">{formatDate(ins.data_inscricao)}</td>
                      <td className="text-xs text-text-muted">{ins.dias_inscricao ?? '—'}</td>
                      <td className="text-right font-semibold text-text-primary">{formatBRL(ins.valor_numerico)}</td>
                      <td className="text-xs text-text-muted">{ins.tipo_garantia}</td>
                      <td>
                        {ins.indicador_ajuizado ? (
                          <CheckCircle className="w-3.5 h-3.5 text-danger" />
                        ) : (
                          <span className="text-text-faint text-xs">Não</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* INTERAÇÕES */}
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
                  <button onClick={registrarInteracao} disabled={!novaInteracao.resumo.trim() || saving}
                    className="btn-primary text-xs">
                    {saving ? 'Salvando…' : 'Salvar'}
                  </button>
                  <button onClick={() => setShowInteracaoForm(false)} className="btn-secondary text-xs">
                    Cancelar
                  </button>
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
                      {(int.responsavel as any)?.nome || 'Sistema'} · {int.estagio_na_interacao ? STAGE_LABELS[int.estagio_na_interacao] : ''}
                    </p>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* PROPOSTA */}
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
