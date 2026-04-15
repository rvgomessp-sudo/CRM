'use client'
// app/(painel)/solver/page.tsx
import { useState } from 'react'
import { formatBRL } from '@/lib/utils'

interface Cenario {
  nome: string
  taxa: number
  comissao: number
  honorarios: number
}

const CENARIOS_PADRAO: Cenario[] = [
  { nome: 'Conservador', taxa: 0.50, comissao: 20, honorarios: 0 },
  { nome: 'Referência', taxa: 0.65, comissao: 22, honorarios: 15000 },
  { nome: 'Agressivo', taxa: 0.80, comissao: 25, honorarios: 30000 },
]

export default function SolverPage() {
  const [valorGarantia, setValorGarantia] = useState('')
  const [prazo, setPrazo] = useState('12')
  const [taxaMin, setTaxaMin] = useState('0.50')
  const [taxaMax, setTaxaMax] = useState('1.20')
  const [comissao, setComissao] = useState('22')
  const [honorarios, setHonorarios] = useState('0')
  const [teto, setTeto] = useState('120000')

  const vg = parseFloat(valorGarantia) || 0
  const p = parseInt(prazo) || 12
  const com = parseFloat(comissao) / 100
  const hon = parseFloat(honorarios) || 0
  const tetoVal = parseFloat(teto) || 120000
  const tMin = parseFloat(taxaMin) / 100
  const tMax = parseFloat(taxaMax) / 100

  // Calcular para um taxa
  function calc(taxa: number) {
    const premioBruto = vg * taxa * (p / 12)
    const comissaoBRL = premioBruto * com
    const premioLiquido = premioBruto - comissaoBRL
    const receitaVF = comissaoBRL + hon
    return { taxa, premioBruto, comissaoBRL, premioLiquido, receitaVF, regraOk: receitaVF > premioLiquido }
  }

  // Grade de sensibilidade
  const steps = 8
  const grade = Array.from({ length: steps + 1 }, (_, i) => {
    const taxa = tMin + (tMax - tMin) * (i / steps)
    return calc(taxa)
  })

  // Taxa mínima para atingir o teto
  const taxaParaTeto = vg > 0 ? (tetoVal / (vg * (p / 12) * (com + (hon / (vg * tMin * (p / 12))) || 0))) : 0

  // Resultado na taxa de referência
  const ref = calc((tMin + tMax) / 2)

  return (
    <div>
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 20, fontWeight: 700, marginBottom: 4 }}>VF Solver</h1>
        <p style={{ color: 'var(--color-text-muted)', fontSize: 13 }}>
          Precificação ótima — análise de sensibilidade taxa × receita
        </p>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '300px 1fr', gap: 20, alignItems: 'start' }}>
        {/* Parâmetros */}
        <div>
          <div className="card" style={{ marginBottom: 16 }}>
            <h3 style={{ fontSize: 12, fontWeight: 600, marginBottom: 14, color: 'var(--color-text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>
              Operação
            </h3>
            {[
              { label: 'Valor da Garantia (R$)', val: valorGarantia, set: setValorGarantia, placeholder: 'Ex: 5000000' },
              { label: 'Prazo (meses)', val: prazo, set: setPrazo, placeholder: '12' },
            ].map(({ label, val, set, placeholder }) => (
              <div key={label} style={{ marginBottom: 12 }}>
                <label style={{ display: 'block', fontSize: 11, color: 'var(--color-text-muted)', marginBottom: 5 }}>{label}</label>
                <input type="number" value={val} onChange={e => set(e.target.value)} placeholder={placeholder} />
              </div>
            ))}
          </div>

          <div className="card" style={{ marginBottom: 16 }}>
            <h3 style={{ fontSize: 12, fontWeight: 600, marginBottom: 14, color: 'var(--color-text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>
              Comissão & Honorários
            </h3>
            {[
              { label: 'Comissão (%)', val: comissao, set: setComissao },
              { label: 'Honorários (R$)', val: honorarios, set: setHonorarios },
              { label: 'Teto de percepção (R$)', val: teto, set: setTeto },
            ].map(({ label, val, set }) => (
              <div key={label} style={{ marginBottom: 12 }}>
                <label style={{ display: 'block', fontSize: 11, color: 'var(--color-text-muted)', marginBottom: 5 }}>{label}</label>
                <input type="number" step="0.01" value={val} onChange={e => set(e.target.value)} />
              </div>
            ))}
          </div>

          <div className="card">
            <h3 style={{ fontSize: 12, fontWeight: 600, marginBottom: 14, color: 'var(--color-text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>
              Faixa de Taxa
            </h3>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
              <div>
                <label style={{ display: 'block', fontSize: 11, color: 'var(--color-text-muted)', marginBottom: 5 }}>Mínima (% a.a.)</label>
                <input type="number" step="0.01" value={taxaMin} onChange={e => setTaxaMin(e.target.value)} />
              </div>
              <div>
                <label style={{ display: 'block', fontSize: 11, color: 'var(--color-text-muted)', marginBottom: 5 }}>Máxima (% a.a.)</label>
                <input type="number" step="0.01" value={taxaMax} onChange={e => setTaxaMax(e.target.value)} />
              </div>
            </div>
          </div>
        </div>

        {/* Resultado */}
        <div>
          {vg > 0 ? (
            <>
              {/* KPIs rápidos na taxa média */}
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12, marginBottom: 20 }}>
                {[
                  { label: 'Prêmio Bruto', value: formatBRL(ref.premioBruto), sub: `@ ${((ref.taxa) * 100).toFixed(2)}% a.a.` },
                  { label: 'Comissão VF', value: formatBRL(ref.comissaoBRL), sub: `${comissao}%` },
                  { label: 'Receita VF', value: formatBRL(ref.receitaVF), sub: ref.regraOk ? '✓ regra OK' : '✗ revisar', danger: !ref.regraOk },
                  { label: 'vs. Teto', value: `${Math.round((ref.receitaVF / tetoVal) * 100)}%`, sub: formatBRL(tetoVal), danger: ref.receitaVF > tetoVal },
                ].map(k => (
                  <div key={k.label} className="kpi-card">
                    <div className="kpi-label">{k.label}</div>
                    <div className="kpi-value" style={{ fontSize: 18, color: k.danger ? '#f87171' : undefined }}>{k.value}</div>
                    <div className="kpi-sub">{k.sub}</div>
                  </div>
                ))}
              </div>

              {/* Grade de sensibilidade */}
              <div className="card" style={{ marginBottom: 20 }}>
                <h3 style={{ fontSize: 13, fontWeight: 600, marginBottom: 16, color: 'var(--color-text-muted)' }}>
                  GRADE DE SENSIBILIDADE — TAXA × RECEITA
                </h3>
                <div style={{ overflowX: 'auto' }}>
                  <table>
                    <thead>
                      <tr>
                        <th>Taxa a.a.</th>
                        <th>Prêmio Bruto</th>
                        <th>Comissão VF</th>
                        <th>Prêmio Líq.</th>
                        <th>Receita VF</th>
                        <th>vs. Teto</th>
                        <th>Regra Econ.</th>
                      </tr>
                    </thead>
                    <tbody>
                      {grade.map((row, i) => {
                        const isRef = i === Math.floor(steps / 2)
                        const tetoOk = row.receitaVF <= tetoVal
                        return (
                          <tr key={i} style={isRef ? { background: 'rgba(59,130,246,0.08)' } : {}}>
                            <td style={{ fontWeight: 700, fontFamily: 'monospace' }}>
                              {(row.taxa * 100).toFixed(2)}%
                              {isRef && <span style={{ marginLeft: 6, fontSize: 10, color: '#60a5fa' }}>← ref</span>}
                            </td>
                            <td>{formatBRL(row.premioBruto)}</td>
                            <td>{formatBRL(row.comissaoBRL)}</td>
                            <td>{formatBRL(row.premioLiquido)}</td>
                            <td style={{ fontWeight: 700, color: row.regraOk ? '#10b981' : '#f87171' }}>
                              {formatBRL(row.receitaVF)}
                            </td>
                            <td style={{ color: tetoOk ? 'var(--color-text-muted)' : '#fbbf24' }}>
                              {Math.round((row.receitaVF / tetoVal) * 100)}%
                            </td>
                            <td style={{ textAlign: 'center' }}>
                              {row.regraOk
                                ? <span style={{ color: '#10b981', fontSize: 14 }}>✓</span>
                                : <span style={{ color: '#f87171', fontSize: 14 }}>✗</span>
                              }
                            </td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              </div>

              {/* Cenários comparativos */}
              <div className="card">
                <h3 style={{ fontSize: 13, fontWeight: 600, marginBottom: 16, color: 'var(--color-text-muted)' }}>
                  CENÁRIOS COMPARATIVOS
                </h3>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 12 }}>
                  {CENARIOS_PADRAO.map(cen => {
                    const r = calc(cen.taxa / 100)
                    const extraCom = vg * (cen.taxa / 100) * (p / 12) * (cen.comissao / 100)
                    const extraHon = cen.honorarios
                    const receitaTotal = extraCom + extraHon
                    const premLiq = vg * (cen.taxa / 100) * (p / 12) * (1 - cen.comissao / 100)
                    const ok = receitaTotal > premLiq
                    return (
                      <div key={cen.nome} style={{
                        border: `1px solid ${ok ? 'rgba(16,185,129,0.2)' : 'var(--color-border)'}`,
                        borderRadius: 8,
                        padding: 14,
                        background: ok ? 'rgba(16,185,129,0.04)' : 'var(--color-surface-2)',
                      }}>
                        <div style={{ fontSize: 12, fontWeight: 700, marginBottom: 8 }}>{cen.nome}</div>
                        <div style={{ fontSize: 11, color: 'var(--color-text-muted)', marginBottom: 4 }}>
                          Taxa: {cen.taxa}% · Com: {cen.comissao}%
                        </div>
                        <div style={{ fontSize: 20, fontWeight: 800, color: ok ? '#10b981' : '#f87171' }}>
                          {formatBRL(receitaTotal)}
                        </div>
                        <div style={{ fontSize: 11, marginTop: 4, color: ok ? '#10b981' : '#f87171' }}>
                          {ok ? '✓ regra OK' : '✗ rever taxa'}
                        </div>
                      </div>
                    )
                  })}
                </div>
              </div>
            </>
          ) : (
            <div className="card" style={{ padding: 48, textAlign: 'center' }}>
              <div style={{ fontSize: 32, marginBottom: 12 }}>◈</div>
              <div style={{ fontSize: 15, fontWeight: 600, marginBottom: 8 }}>VF Solver</div>
              <div style={{ color: 'var(--color-text-muted)', fontSize: 13 }}>
                Informe o valor da garantia para iniciar a análise
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
