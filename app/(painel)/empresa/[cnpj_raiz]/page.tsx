'use client'
// app/(painel)/empresa/[cnpj_raiz]/page.tsx
import { useEffect, useState } from 'react'
import { useParams, useRouter } from 'next/navigation'
import { createClient } from '@/lib/supabase/client'
import { Empresa, Inscricao, Interacao, ESTAGIO_LABELS, ESTAGIO_ORDEM, EstagioPipeline } from '@/lib/types'
import { formatBRL, podeProceder } from '@/lib/utils'

type Aba = 'visao_geral' | 'inscricoes' | 'interacoes' | 'seguradora' | 'proposta'

export default function EmpresaPage() {
  const { cnpj_raiz } = useParams<{ cnpj_raiz: string }>()
  const router = useRouter()
  const [empresa, setEmpresa] = useState<Empresa | null>(null)
  const [inscricoes, setInscricoes] = useState<Inscricao[]>([])
  const [interacoes, setInteracoes] = useState<Interacao[]>([])
  const [aba, setAba] = useState<Aba>('visao_geral')
  const [loading, setLoading] = useState(true)
  const [novaInteracao, setNovaInteracao] = useState({ canal: 'email', resumo: '', proxima_acao: '', data_proxima_acao: '' })
  const supabase = createClient()

  async function carregar() {
    const [{ data: emp }, { data: insc }, { data: inter }] = await Promise.all([
      supabase.from('empresas').select('*').eq('cnpj_raiz', cnpj_raiz).single(),
      supabase.from('inscricoes').select('*').eq('cnpj_raiz', cnpj_raiz).order('valor_numerico', { ascending: false }),
      supabase.from('interacoes').select('*, usuario:usuarios(nome)').eq('cnpj_raiz', cnpj_raiz).order('criado_em', { ascending: false }),
    ])
    setEmpresa(emp)
    setInscricoes(insc || [])
    setInteracoes(inter || [])
    setLoading(false)
  }

  useEffect(() => { carregar() }, [cnpj_raiz])

  async function moverEstagio(novoEstagio: EstagioPipeline) {
    if (!empresa) return
    const { ok, camposFaltando } = podeProceder(empresa, novoEstagio)
    if (!ok) { alert(`Campos obrigatórios: ${camposFaltando.join(', ')}`); return }
    await supabase.from('empresas').update({ estagio: novoEstagio }).eq('cnpj_raiz', cnpj_raiz)
    await supabase.from('interacoes').insert({
      cnpj_raiz,
      canal: 'sistema',
      resumo: `Avançou para: ${ESTAGIO_LABELS[novoEstagio]}`,
      estagio_momento: novoEstagio,
    })
    carregar()
  }

  async function salvarInteracao() {
    if (!novaInteracao.resumo) return
    await supabase.from('interacoes').insert({
      cnpj_raiz,
      canal: novaInteracao.canal,
      resumo: novaInteracao.resumo,
      proxima_acao: novaInteracao.proxima_acao || null,
      data_proxima_acao: novaInteracao.data_proxima_acao || null,
      estagio_momento: empresa?.estagio,
    })
    setNovaInteracao({ canal: 'email', resumo: '', proxima_acao: '', data_proxima_acao: '' })
    carregar()
  }

  if (loading) return <div style={{ padding: 40, color: 'var(--color-text-muted)' }}>Carregando...</div>
  if (!empresa) return <div style={{ padding: 40, color: '#f87171' }}>Empresa não encontrada.</div>

  const idxAtual = ESTAGIO_ORDEM.indexOf(empresa.estagio)
  const proximoEstagio = ESTAGIO_ORDEM[idxAtual + 1]
  const anteriorEstagio = ESTAGIO_ORDEM[idxAtual - 1]

  const ABAS: { id: Aba; label: string }[] = [
    { id: 'visao_geral', label: 'Visão Geral' },
    { id: 'inscricoes', label: `Inscrições (${inscricoes.length})` },
    { id: 'interacoes', label: `Histórico (${interacoes.length})` },
    { id: 'seguradora', label: 'Seguradora' },
    { id: 'proposta', label: 'Proposta' },
  ]

  return (
    <div>
      {/* Header */}
      <div style={{ marginBottom: 20 }}>
        <button
          onClick={() => router.back()}
          style={{ background: 'none', border: 'none', color: 'var(--color-text-muted)', cursor: 'pointer', fontSize: 13, marginBottom: 12 }}
        >
          ← Voltar
        </button>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
          <div>
            <h1 style={{ fontSize: 22, fontWeight: 700, marginBottom: 4 }}>{empresa.nome_devedor}</h1>
            <div style={{ display: 'flex', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
              <span style={{ fontFamily: 'monospace', fontSize: 13, color: 'var(--color-text-muted)' }}>
                {empresa.cnpj_completo || empresa.cnpj_raiz}
              </span>
              <span className={`badge badge-${(empresa.prioridade || 'baixa').toLowerCase()}`}>
                {empresa.prioridade}
              </span>
              <span className={`badge badge-${(empresa.seguradora || '').toLowerCase().replace(/\s/g, '')}`}>
                {empresa.seguradora}
              </span>
              <span style={{ fontSize: 13, fontWeight: 600, color: '#60a5fa' }}>
                {formatBRL(empresa.valor_total_brl)}
              </span>
            </div>
          </div>
          {/* Controles de estágio */}
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            {anteriorEstagio && (
              <button className="btn btn-ghost" onClick={() => moverEstagio(anteriorEstagio)} style={{ fontSize: 12 }}>
                ← Voltar etapa
              </button>
            )}
            <div style={{
              padding: '6px 14px',
              background: 'var(--color-surface-2)',
              border: '1px solid var(--color-border)',
              borderRadius: 6,
              fontSize: 13,
              fontWeight: 600,
            }}>
              {ESTAGIO_LABELS[empresa.estagio]}
            </div>
            {proximoEstagio && (
              <button className="btn btn-primary" onClick={() => moverEstagio(proximoEstagio)} style={{ fontSize: 12 }}>
                Avançar →
              </button>
            )}
          </div>
        </div>
      </div>

      {/* Progress bar do pipeline */}
      <div style={{ display: 'flex', gap: 2, marginBottom: 20, height: 4 }}>
        {ESTAGIO_ORDEM.map((e, i) => (
          <div key={e} style={{
            flex: 1,
            background: i <= idxAtual ? '#3b82f6' : 'var(--color-border)',
            borderRadius: 2,
          }} />
        ))}
      </div>

      {/* Abas */}
      <div style={{ display: 'flex', gap: 2, borderBottom: '1px solid var(--color-border)', marginBottom: 20 }}>
        {ABAS.map(a => (
          <button
            key={a.id}
            onClick={() => setAba(a.id)}
            style={{
              padding: '8px 16px',
              background: 'none',
              border: 'none',
              borderBottom: aba === a.id ? '2px solid #3b82f6' : '2px solid transparent',
              color: aba === a.id ? 'var(--color-text)' : 'var(--color-text-muted)',
              cursor: 'pointer',
              fontSize: 13,
              fontWeight: aba === a.id ? 600 : 400,
            }}
          >
            {a.label}
          </button>
        ))}
      </div>

      {/* Conteúdo das abas */}
      {aba === 'visao_geral' && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
          <div className="card">
            <h3 style={{ fontSize: 13, fontWeight: 600, marginBottom: 12, color: 'var(--color-text-muted)' }}>DADOS CADASTRAIS</h3>
            <table style={{ fontSize: 13 }}>
              <tbody>
                {[
                  ['CNPJ Raiz', empresa.cnpj_raiz],
                  ['CNPJ Completo', empresa.cnpj_completo || '—'],
                  ['UF', empresa.uf_devedor || '—'],
                  ['Regime', empresa.regime_tributario || '—'],
                  ['PL Estimado', formatBRL(empresa.pl_estimado)],
                  ['Receita Estimada', formatBRL(empresa.receita_estimada)],
                  ['Fonte', empresa.fonte || '—'],
                ].map(([label, value]) => (
                  <tr key={label}>
                    <td style={{ color: 'var(--color-text-muted)', paddingRight: 16, paddingBottom: 6 }}>{label}</td>
                    <td style={{ fontWeight: 500, paddingBottom: 6 }}>{value}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="card">
            <h3 style={{ fontSize: 13, fontWeight: 600, marginBottom: 12, color: 'var(--color-text-muted)' }}>RESUMO FINANCEIRO</h3>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
              {[
                { label: 'Valor Total', value: formatBRL(empresa.valor_total_brl) },
                { label: 'Inscrições', value: String(empresa.qtd_inscricoes_empresa || 0) },
                { label: 'Menor Inscrição', value: formatBRL(empresa.valor_minimo_inscricao) },
                { label: 'Maior Inscrição', value: formatBRL(empresa.valor_maximo_inscricao) },
              ].map(kpi => (
                <div key={kpi.label} style={{ background: 'var(--color-surface-2)', borderRadius: 6, padding: '10px 12px' }}>
                  <div style={{ fontSize: 10, color: 'var(--color-text-muted)', marginBottom: 4, textTransform: 'uppercase' }}>{kpi.label}</div>
                  <div style={{ fontSize: 18, fontWeight: 700 }}>{kpi.value}</div>
                </div>
              ))}
            </div>
          </div>

          <div className="card" style={{ gridColumn: '1 / -1' }}>
            <h3 style={{ fontSize: 13, fontWeight: 600, marginBottom: 12, color: 'var(--color-text-muted)' }}>CONSULTA SANCOR</h3>
            <div style={{ display: 'flex', gap: 20, flexWrap: 'wrap' }}>
              {[
                { label: 'Consultado', value: empresa.sancor_consultado ? '✓ Sim' : '✗ Não', ok: empresa.sancor_consultado },
                { label: 'Aprovado', value: empresa.sancor_aprovado === true ? '✓ Sim' : empresa.sancor_aprovado === false ? '✗ Não' : '—', ok: empresa.sancor_aprovado },
                { label: 'Limite', value: formatBRL(empresa.sancor_limite) },
                { label: 'Taxa Mínima', value: empresa.sancor_taxa_minima ? `${(empresa.sancor_taxa_minima * 100).toFixed(2)}% a.a.` : '—' },
                { label: 'Data Consulta', value: empresa.sancor_data_consulta || '—' },
              ].map(item => (
                <div key={item.label} style={{ minWidth: 120 }}>
                  <div style={{ fontSize: 10, color: 'var(--color-text-muted)', marginBottom: 2 }}>{item.label}</div>
                  <div style={{ fontWeight: 600, color: 'ok' in item ? (item.ok ? '#10b981' : '#f87171') : undefined }}>
                    {item.value}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {aba === 'inscricoes' && (
        <div className="card" style={{ padding: 0 }}>
          <div style={{ padding: '14px 16px', borderBottom: '1px solid var(--color-border)' }}>
            <h3 style={{ fontSize: 14, fontWeight: 600 }}>Inscrições Individuais PGFN</h3>
            <p style={{ fontSize: 12, color: 'var(--color-text-muted)', marginTop: 2 }}>
              {inscricoes.length} inscrições · Total: {formatBRL(inscricoes.reduce((acc, i) => acc + (i.valor_numerico || 0), 0))}
            </p>
          </div>
          <div style={{ overflowX: 'auto' }}>
            <table>
              <thead>
                <tr>
                  <th>Nº Inscrição</th>
                  <th>Tributo</th>
                  <th>Situação</th>
                  <th>Garantia</th>
                  <th>Data</th>
                  <th>Dias</th>
                  <th>Valor</th>
                  <th>Ajuizado</th>
                </tr>
              </thead>
              <tbody>
                {inscricoes.map(insc => (
                  <tr key={insc.id}>
                    <td style={{ fontFamily: 'monospace', fontSize: 11 }}>{insc.numero_inscricao}</td>
                    <td>
                      <span style={{
                        padding: '2px 8px',
                        background: insc.tributo === 'IRPJ' ? 'rgba(59,130,246,0.15)' : insc.tributo === 'PIS' ? 'rgba(16,185,129,0.15)' : 'rgba(168,85,247,0.15)',
                        color: insc.tributo === 'IRPJ' ? '#60a5fa' : insc.tributo === 'PIS' ? '#34d399' : '#c084fc',
                        borderRadius: 10,
                        fontSize: 11,
                        fontWeight: 600,
                      }}>
                        {insc.tributo}
                      </span>
                    </td>
                    <td style={{ fontSize: 12 }}>{insc.situacao_inscricao}</td>
                    <td style={{ fontSize: 12, color: insc.flag_garantia === 'NAO' ? '#f87171' : '#34d399' }}>
                      {insc.flag_garantia === 'NAO' ? 'Sem Garantia' : insc.tipo_garantia}
                    </td>
                    <td style={{ fontSize: 12 }}>{insc.data_inscricao}</td>
                    <td style={{ fontSize: 12, textAlign: 'center' }}>{insc.dias_inscricao}</td>
                    <td style={{ fontWeight: 700 }}>{formatBRL(insc.valor_numerico)}</td>
                    <td style={{ textAlign: 'center' }}>
                      {insc.indicador_ajuizado
                        ? <span style={{ color: '#f87171', fontSize: 13 }}>✓</span>
                        : <span style={{ color: 'var(--color-text-muted)', fontSize: 13 }}>—</span>
                      }
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {aba === 'interacoes' && (
        <div>
          {/* Nova interação */}
          <div className="card" style={{ marginBottom: 16 }}>
            <h3 style={{ fontSize: 13, fontWeight: 600, marginBottom: 12, color: 'var(--color-text-muted)' }}>REGISTRAR INTERAÇÃO</h3>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr 1fr 1fr', gap: 10, marginBottom: 10 }}>
              <select value={novaInteracao.canal} onChange={e => setNovaInteracao(p => ({ ...p, canal: e.target.value }))}>
                <option value="email">Email</option>
                <option value="telefone">Telefone</option>
                <option value="whatsapp">WhatsApp</option>
                <option value="reuniao">Reunião</option>
                <option value="outro">Outro</option>
              </select>
              <input
                placeholder="Resumo do contato..."
                value={novaInteracao.resumo}
                onChange={e => setNovaInteracao(p => ({ ...p, resumo: e.target.value }))}
              />
              <input
                placeholder="Próxima ação"
                value={novaInteracao.proxima_acao}
                onChange={e => setNovaInteracao(p => ({ ...p, proxima_acao: e.target.value }))}
              />
              <input
                type="date"
                value={novaInteracao.data_proxima_acao}
                onChange={e => setNovaInteracao(p => ({ ...p, data_proxima_acao: e.target.value }))}
              />
            </div>
            <button className="btn btn-primary" onClick={salvarInteracao}>Salvar</button>
          </div>

          {/* Timeline */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
            {interacoes.map(inter => (
              <div key={inter.id} className="card-sm" style={{ display: 'flex', gap: 12 }}>
                <div style={{
                  width: 36, height: 36, borderRadius: '50%',
                  background: 'var(--color-surface-2)',
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  fontSize: 14, flexShrink: 0,
                }}>
                  {inter.canal === 'email' ? '✉' : inter.canal === 'telefone' ? '☎' : inter.canal === 'whatsapp' ? '💬' : inter.canal === 'reuniao' ? '👥' : '○'}
                </div>
                <div style={{ flex: 1 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                    <span style={{ fontSize: 12, fontWeight: 600 }}>{inter.resumo}</span>
                    <span style={{ fontSize: 11, color: 'var(--color-text-muted)' }}>
                      {new Date(inter.criado_em).toLocaleString('pt-BR')}
                    </span>
                  </div>
                  {inter.proxima_acao && (
                    <div style={{ fontSize: 11, color: '#fbbf24' }}>
                      ⏰ {inter.proxima_acao} — {inter.data_proxima_acao}
                    </div>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {aba === 'seguradora' && (
        <SeguradoraAba empresa={empresa} onAtualizar={carregar} />
      )}

      {aba === 'proposta' && (
        <PropostaAba empresa={empresa} />
      )}
    </div>
  )
}

// ── Aba Seguradora ────────────────────────────────────────
function SeguradoraAba({ empresa, onAtualizar }: { empresa: Empresa; onAtualizar: () => void }) {
  const supabase = createClient()
  const [form, setForm] = useState({
    sancor_aprovado: empresa.sancor_aprovado === true ? 'true' : empresa.sancor_aprovado === false ? 'false' : '',
    sancor_limite: String(empresa.sancor_limite || ''),
    sancor_taxa_minima: String(empresa.sancor_taxa_minima ? empresa.sancor_taxa_minima * 100 : ''),
    sancor_data_consulta: empresa.sancor_data_consulta || '',
    pl_estimado: String(empresa.pl_estimado || ''),
    receita_estimada: String(empresa.receita_estimada || ''),
    regime_tributario: empresa.regime_tributario || '',
  })

  async function salvar() {
    await supabase.from('empresas').update({
      sancor_consultado: true,
      sancor_aprovado: form.sancor_aprovado === 'true' ? true : form.sancor_aprovado === 'false' ? false : null,
      sancor_limite: parseFloat(form.sancor_limite) || null,
      sancor_taxa_minima: parseFloat(form.sancor_taxa_minima) / 100 || null,
      sancor_data_consulta: form.sancor_data_consulta || null,
      pl_estimado: parseFloat(form.pl_estimado) || null,
      receita_estimada: parseFloat(form.receita_estimada) || null,
      regime_tributario: form.regime_tributario || null,
    }).eq('cnpj_raiz', empresa.cnpj_raiz)
    onAtualizar()
  }

  return (
    <div className="card" style={{ maxWidth: 600 }}>
      <h3 style={{ fontSize: 13, fontWeight: 600, marginBottom: 16, color: 'var(--color-text-muted)' }}>CONSULTA SANCOR</h3>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 14 }}>
        {[
          { label: 'Resultado Consulta', field: 'sancor_aprovado', type: 'select', options: [['', 'Pendente'], ['true', 'Aprovado'], ['false', 'Reprovado']] },
          { label: 'Limite Aprovado (R$)', field: 'sancor_limite', type: 'number' },
          { label: 'Taxa Mínima (% a.a.)', field: 'sancor_taxa_minima', type: 'number' },
          { label: 'Data da Consulta', field: 'sancor_data_consulta', type: 'date' },
          { label: 'PL Estimado (R$)', field: 'pl_estimado', type: 'number' },
          { label: 'Receita Estimada (R$)', field: 'receita_estimada', type: 'number' },
        ].map(({ label, field, type, options }) => (
          <div key={field}>
            <label style={{ display: 'block', fontSize: 11, color: 'var(--color-text-muted)', marginBottom: 6, textTransform: 'uppercase' }}>{label}</label>
            {type === 'select' ? (
              <select value={(form as Record<string, string>)[field]} onChange={e => setForm(p => ({ ...p, [field]: e.target.value }))}>
                {(options || []).map(([val, lbl]) => <option key={val} value={val}>{lbl}</option>)}
              </select>
            ) : (
              <input
                type={type}
                value={(form as Record<string, string>)[field]}
                onChange={e => setForm(p => ({ ...p, [field]: e.target.value }))}
              />
            )}
          </div>
        ))}
        <div>
          <label style={{ display: 'block', fontSize: 11, color: 'var(--color-text-muted)', marginBottom: 6, textTransform: 'uppercase' }}>Regime Tributário</label>
          <select value={form.regime_tributario} onChange={e => setForm(p => ({ ...p, regime_tributario: e.target.value }))}>
            <option value="">Selecione</option>
            <option value="Lucro Real">Lucro Real</option>
            <option value="Lucro Presumido">Lucro Presumido</option>
            <option value="Simples Nacional">Simples Nacional</option>
          </select>
        </div>
      </div>
      <button className="btn btn-primary" onClick={salvar} style={{ marginTop: 16 }}>Salvar</button>
    </div>
  )
}

// ── Aba Proposta / VF Solver ──────────────────────────────
function PropostaAba({ empresa }: { empresa: Empresa }) {
  const [form, setForm] = useState({
    valor_garantia: String(empresa.valor_total_brl || ''),
    taxa_aa: '0.50',
    prazo_meses: '12',
    comissao_pct: '22',
    honorarios_brl: '0',
  })

  const vg = parseFloat(form.valor_garantia) || 0
  const taxa = parseFloat(form.taxa_aa) / 100
  const prazo = parseInt(form.prazo_meses) || 12
  const comissao = parseFloat(form.comissao_pct) / 100
  const honorarios = parseFloat(form.honorarios_brl) || 0

  const premioBruto = vg * taxa * (prazo / 12)
  const comissaoBRL = premioBruto * comissao
  const premioLiquido = premioBruto - comissaoBRL
  const receitaVF = comissaoBRL + honorarios
  const regraOk = receitaVF > premioLiquido

  return (
    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
      <div className="card">
        <h3 style={{ fontSize: 13, fontWeight: 600, marginBottom: 16, color: 'var(--color-text-muted)' }}>PARÂMETROS</h3>
        {[
          { label: 'Valor da Garantia (R$)', field: 'valor_garantia' },
          { label: 'Taxa a.a. (%)', field: 'taxa_aa' },
          { label: 'Prazo (meses)', field: 'prazo_meses' },
          { label: 'Comissão (%)', field: 'comissao_pct' },
          { label: 'Honorários (R$)', field: 'honorarios_brl' },
        ].map(({ label, field }) => (
          <div key={field} style={{ marginBottom: 12 }}>
            <label style={{ display: 'block', fontSize: 11, color: 'var(--color-text-muted)', marginBottom: 5, textTransform: 'uppercase' }}>{label}</label>
            <input
              type="number"
              step="0.01"
              value={(form as Record<string, string>)[field]}
              onChange={e => setForm(p => ({ ...p, [field]: e.target.value }))}
            />
          </div>
        ))}
      </div>

      <div className="card">
        <h3 style={{ fontSize: 13, fontWeight: 600, marginBottom: 16, color: 'var(--color-text-muted)' }}>RESULTADO</h3>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          {[
            { label: 'Prêmio Bruto', value: formatBRL(premioBruto) },
            { label: 'Comissão VF', value: formatBRL(comissaoBRL) },
            { label: 'Prêmio Líquido (cliente)', value: formatBRL(premioLiquido) },
            { label: 'Honorários', value: formatBRL(honorarios) },
          ].map(item => (
            <div key={item.label} style={{ display: 'flex', justifyContent: 'space-between', padding: '8px 0', borderBottom: '1px solid var(--color-border)' }}>
              <span style={{ color: 'var(--color-text-muted)', fontSize: 13 }}>{item.label}</span>
              <span style={{ fontWeight: 600 }}>{item.value}</span>
            </div>
          ))}
          <div style={{
            marginTop: 8,
            padding: '12px 16px',
            borderRadius: 8,
            background: regraOk ? 'rgba(16,185,129,0.1)' : 'rgba(239,68,68,0.1)',
            border: `1px solid ${regraOk ? 'rgba(16,185,129,0.2)' : 'rgba(239,68,68,0.2)'}`,
          }}>
            <div style={{ fontSize: 11, marginBottom: 4, color: 'var(--color-text-muted)' }}>RECEITA V&F TOTAL</div>
            <div style={{ fontSize: 28, fontWeight: 800, color: regraOk ? '#10b981' : '#f87171' }}>
              {formatBRL(receitaVF)}
            </div>
            <div style={{ fontSize: 12, marginTop: 4, color: regraOk ? '#10b981' : '#f87171' }}>
              {regraOk ? '✓ Regra econômica OK: Receita VF > Prêmio Líquido' : '✗ ATENÇÃO: Receita VF ≤ Prêmio Líquido — revisar taxa'}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
