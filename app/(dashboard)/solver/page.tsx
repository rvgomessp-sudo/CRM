'use client'

import { Suspense, useEffect, useState, useCallback } from 'react'
import { useSearchParams } from 'next/navigation'
import { createClient } from '@/lib/supabase/client'
import { formatBRL, formatTaxa, cn } from '@/lib/utils'
import { Calculator, CheckCircle, AlertTriangle, TrendingUp, Save } from 'lucide-react'
import type { SeguradoraTipo } from '@/lib/types'

// Parâmetros por seguradora
const SEGURADORA_PARAMS: Record<SeguradoraTipo, {
  taxaMin: number; taxaMax: number; taxaDefault: number
  comissaoMin: number; comissaoMax: number; comissaoDefault: number
  limiteMax: number; label: string
}> = {
  SANCOR:  { taxaMin: 0.0050, taxaMax: 0.0200, taxaDefault: 0.0070, comissaoMin: 0.20, comissaoMax: 0.25, comissaoDefault: 0.22, limiteMax: 20_000_000, label: 'Sancor (≤ R$ 20M)' },
  BERKLEY: { taxaMin: 0.0045, taxaMax: 0.0180, taxaDefault: 0.0065, comissaoMin: 0.18, comissaoMax: 0.23, comissaoDefault: 0.20, limiteMax: 30_000_000, label: 'Berkley (≤ R$ 30M)' },
  ZURICH:  { taxaMin: 0.0040, taxaMax: 0.0160, taxaDefault: 0.0060, comissaoMin: 0.15, comissaoMax: 0.22, comissaoDefault: 0.18, limiteMax: 999_999_999, label: 'Zurich (> R$ 30M)' },
  SWISS:   { taxaMin: 0.0040, taxaMax: 0.0160, taxaDefault: 0.0060, comissaoMin: 0.15, comissaoMax: 0.22, comissaoDefault: 0.18, limiteMax: 999_999_999, label: 'Swiss Re (> R$ 30M)' },
  CHUBB:   { taxaMin: 0.0045, taxaMax: 0.0170, taxaDefault: 0.0065, comissaoMin: 0.16, comissaoMax: 0.22, comissaoDefault: 0.19, limiteMax: 999_999_999, label: 'Chubb (> R$ 30M)' },
}

// Teto de percepção V&F
const TETO_MIN = 80_000
const TETO_MAX = 200_000

function calcular(
  valorGarantia: number,
  taxaAnual: number,
  prazoAnos: number,
  comissaoPct: number,
  honorarios: number
) {
  const premioBruto = valorGarantia * taxaAnual * prazoAnos
  const comissaoValor = premioBruto * comissaoPct
  const receiaVFTotal = comissaoValor + honorarios
  const regraEconomica = receiaVFTotal >= (TETO_MIN * 0.5) // mínimo meia percepção mínima
  const dentroTeto = receiaVFTotal >= TETO_MIN && receiaVFTotal <= TETO_MAX * 1.5

  const alertas: string[] = []
  if (receiaVFTotal < TETO_MIN) alertas.push(`Receita V&F (${formatBRL(receiaVFTotal)}) abaixo do teto mínimo de ${formatBRL(TETO_MIN)}`)
  if (receiaVFTotal > TETO_MAX) alertas.push(`Receita V&F (${formatBRL(receiaVFTotal)}) acima do teto-alvo de ${formatBRL(TETO_MAX)} — revisar honorários`)
  if (premioBruto > receiaVFTotal) alertas.push('Prêmio bruto > receita V&F — avaliar incremento de honorários')

  return { premioBruto, comissaoValor, receiaVFTotal, regraEconomica, alertas }
}

export default function SolverPage() {
  // useSearchParams exige Suspense no App Router
  return (
    <Suspense>
      <SolverInner />
    </Suspense>
  )
}

function SolverInner() {
  const supabase = createClient()
  const sp = useSearchParams()

  // Inputs
  const [seguradora, setSeguradora] = useState<SeguradoraTipo>('SANCOR')
  const [valorGarantia, setValorGarantia] = useState<string>('5000000')
  const [taxaAnual, setTaxaAnual] = useState<string>('0.70')         // % (0.70 = 0.70% = 0.0070)
  const [prazoAnos, setPrazoAnos] = useState<string>('1')
  const [comissaoPct, setComissaoPct] = useState<string>('22')       // % (22 = 22% = 0.22)
  const [honorarios, setHonorarios] = useState<string>('0')
  const [cnpjRef, setCnpjRef] = useState<string>('')
  const [empresaNome, setEmpresaNome] = useState<string>('')
  const [saving, setSaving] = useState(false)
  const [saved, setSaved] = useState(false)

  // A ficha da empresa chama /solver?cnpj=<raiz> — preencher tudo que dá
  useEffect(() => {
    const cnpj = sp.get('cnpj')
    if (!cnpj) return
    setCnpjRef(cnpj)
    supabase.from('vw_fila_oportunidades')
      .select('nome_devedor, valor_total_devida')
      .eq('cnpj_raiz', cnpj.replace(/\D/g, '').slice(0, 8)).limit(1)
      .then(({ data }) => {
        const emp = data?.[0]
        if (emp) {
          setEmpresaNome(emp.nome_devedor)
          if (emp.valor_total_devida) setValorGarantia(String(Math.round(emp.valor_total_devida)))
        }
      })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const params = SEGURADORA_PARAMS[seguradora]

  // Calcula resultado
  const vg = parseFloat(valorGarantia.replace(/\D/g, '')) || 0
  const ta = parseFloat(taxaAnual) / 100 || 0
  const pr = parseFloat(prazoAnos) || 1
  const cp = parseFloat(comissaoPct) / 100 || 0
  const hn = parseFloat(honorarios.replace(/\D/g, '')) || 0

  const resultado = vg > 0 ? calcular(vg, ta, pr, cp, hn) : null

  function applySeguradora(s: SeguradoraTipo) {
    const p = SEGURADORA_PARAMS[s]
    setSeguradora(s)
    setTaxaAnual((p.taxaDefault * 100).toFixed(2))
    setComissaoPct((p.comissaoDefault * 100).toFixed(0))
  }

  async function salvarProposta() {
    if (!resultado || !cnpjRef.trim()) return
    setSaving(true)

    const { data: { user } } = await supabase.auth.getUser()

    await supabase.from('propostas').insert({
      cnpj_raiz:          cnpjRef.trim().padStart(8, '0').slice(0, 8),
      valor_garantia:     vg,
      seguradora,
      taxa_anual:         ta,
      prazo_anos:         pr,
      premio_bruto:       resultado.premioBruto,
      comissao_pct:       cp,
      comissao_valor:     resultado.comissaoValor,
      honorarios_valor:   hn,
      receita_vf_total:   resultado.receiaVFTotal,
      regra_economica_ok: resultado.regraEconomica,
      status:             'RASCUNHO',
      criado_por:         user?.id,
    })

    setSaved(true)
    setSaving(false)
    setTimeout(() => setSaved(false), 3000)
  }

  return (
    <div className="p-6 max-w-4xl mx-auto">
      <div className="flex items-center gap-3 mb-6">
        <Calculator className="w-5 h-5 text-vf-red" />
        <div>
          <h1 className="text-xl font-bold text-text-primary">VF Solver</h1>
          <p className="text-text-muted text-sm">Precificação ótima — regra: comissão + honorários ≥ R$ {(TETO_MIN/1000).toFixed(0)}K</p>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">

        {/* Inputs */}
        <div className="space-y-5">

          {/* Seguradora */}
          <div>
            <label className="label">Seguradora</label>
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-2">
              {(Object.keys(SEGURADORA_PARAMS) as SeguradoraTipo[]).map(s => (
                <button
                  key={s}
                  onClick={() => applySeguradora(s)}
                  className={cn(
                    'px-3 py-2 rounded border text-xs font-medium transition-colors',
                    seguradora === s
                      ? 'bg-vf-red/10 border-vf-red text-vf-red-light'
                      : 'bg-bg-secondary border-border text-text-muted hover:border-border-strong'
                  )}
                >
                  {s}
                </button>
              ))}
            </div>
            <p className="text-text-faint text-[10px] mt-1">{params.label}</p>
          </div>

          {/* Valor da garantia */}
          <div>
            <label className="label">Valor da Garantia (R$)</label>
            <input
              type="number"
              className="input"
              placeholder="5000000"
              value={valorGarantia}
              onChange={e => setValorGarantia(e.target.value)}
            />
            {vg > params.limiteMax && (
              <p className="text-warning text-xs mt-1">
                Valor supera o limite da {seguradora} (R$ {formatBRL(params.limiteMax)})
              </p>
            )}
          </div>

          {/* Taxa anual */}
          <div>
            <label className="label">
              Taxa Anual (%)
              <span className="text-text-faint ml-2">
                Faixa: {(params.taxaMin*100).toFixed(2)}% – {(params.taxaMax*100).toFixed(2)}%
              </span>
            </label>
            <div className="flex items-center gap-2">
              <input
                type="number"
                step="0.01"
                min={params.taxaMin * 100}
                max={params.taxaMax * 100}
                className="input"
                value={taxaAnual}
                onChange={e => setTaxaAnual(e.target.value)}
              />
              <span className="text-text-muted text-sm w-12">% a.a.</span>
            </div>
            <input
              type="range"
              min={params.taxaMin * 100} max={params.taxaMax * 100} step={0.01}
              value={taxaAnual}
              onChange={e => setTaxaAnual(e.target.value)}
              className="w-full mt-1 accent-vf-red"
            />
          </div>

          {/* Prazo */}
          <div>
            <label className="label">Prazo (anos)</label>
            <div className="flex gap-2">
              {[1, 2, 3, 5].map(a => (
                <button
                  key={a}
                  onClick={() => setPrazoAnos(String(a))}
                  className={cn(
                    'px-3 py-1.5 rounded border text-xs font-medium transition-colors',
                    parseFloat(prazoAnos) === a
                      ? 'bg-vf-red/10 border-vf-red text-vf-red-light'
                      : 'bg-bg-secondary border-border text-text-muted hover:border-border-strong'
                  )}
                >
                  {a}a
                </button>
              ))}
              <input
                type="number" step="0.5" min="0.5" max="10"
                className="input py-1 text-sm w-24"
                value={prazoAnos}
                onChange={e => setPrazoAnos(e.target.value)}
              />
            </div>
          </div>

          {/* Comissão */}
          <div>
            <label className="label">
              Comissão (%)
              <span className="text-text-faint ml-2">
                Faixa: {(params.comissaoMin*100).toFixed(0)}% – {(params.comissaoMax*100).toFixed(0)}%
              </span>
            </label>
            <div className="flex items-center gap-2">
              <input
                type="number"
                step="0.5"
                min={params.comissaoMin * 100}
                max={params.comissaoMax * 100}
                className="input"
                value={comissaoPct}
                onChange={e => setComissaoPct(e.target.value)}
              />
              <span className="text-text-muted text-sm w-8">%</span>
            </div>
          </div>

          {/* Honorários */}
          <div>
            <label className="label">Honorários V&F (R$)</label>
            <input
              type="number"
              step="1000"
              min="0"
              className="input"
              placeholder="0"
              value={honorarios}
              onChange={e => setHonorarios(e.target.value)}
            />
            <p className="text-text-faint text-[10px] mt-1">
              Componente fixo ou percentual adicional sobre honorários de estruturação
            </p>
          </div>

          {/* CNPJ para salvar */}
          <div className="pt-3 border-t border-border">
            <label className="label">CNPJ Raiz (8 dígitos) — para vincular à empresa</label>
            <input
              type="text"
              maxLength={8}
              className="input font-mono"
              placeholder="00000000"
              value={cnpjRef}
              onChange={e => setCnpjRef(e.target.value.replace(/\D/g, ''))}
            />
            {empresaNome && (
              <p className="text-xs text-vf-red-light mt-1.5 truncate" title={empresaNome}>
                → {empresaNome} <span className="text-text-faint">(dívida pré-carregada como valor da garantia)</span>
              </p>
            )}
          </div>
        </div>

        {/* Output */}
        <div className="space-y-4">
          <div className="card sticky top-6">
            <p className="section-header mb-4">Resultado</p>

            {!resultado ? (
              <p className="text-text-faint text-sm">Preencha os dados ao lado para calcular.</p>
            ) : (
              <div className="space-y-4">

                {/* Prêmio */}
                <div className="p-3 rounded bg-bg-secondary">
                  <p className="text-text-faint text-xs">Prêmio Bruto</p>
                  <p className="text-2xl font-bold text-text-primary">{formatBRL(resultado.premioBruto)}</p>
                  <p className="text-text-faint text-[10px]">
                    {formatBRL(vg)} × {(ta*100).toFixed(2)}% × {pr}a
                  </p>
                </div>

                {/* Decomposição da receita */}
                <div className="space-y-2">
                  <div className="flex justify-between items-center">
                    <span className="text-text-muted text-sm">Comissão ({(cp*100).toFixed(0)}%)</span>
                    <span className="text-success font-semibold">{formatBRL(resultado.comissaoValor)}</span>
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="text-text-muted text-sm">Honorários</span>
                    <span className="text-info font-semibold">{formatBRL(hn)}</span>
                  </div>
                  <div className="border-t border-border pt-2 flex justify-between items-center">
                    <span className="text-text-primary font-semibold">Receita V&F Total</span>
                    <span className="text-vf-red-light font-bold text-lg">{formatBRL(resultado.receiaVFTotal)}</span>
                  </div>
                </div>

                {/* Regra econômica */}
                <div className={cn(
                  'flex items-center gap-2 p-2.5 rounded border text-sm',
                  resultado.regraEconomica
                    ? 'bg-success/10 border-success/30 text-success'
                    : 'bg-danger/10 border-danger/30 text-danger'
                )}>
                  {resultado.regraEconomica
                    ? <><CheckCircle className="w-4 h-4" /> Regra econômica atendida</>
                    : <><AlertTriangle className="w-4 h-4" /> Regra econômica NÃO atendida</>
                  }
                </div>

                {/* Teto de percepção */}
                <div>
                  <div className="flex justify-between text-xs mb-1">
                    <span className="text-text-muted">Teto percepção</span>
                    <span className="text-text-muted">R$ 80K → R$ 200K</span>
                  </div>
                  <div className="bg-bg-secondary rounded-full h-2 overflow-hidden">
                    <div
                      className={cn(
                        'h-full rounded-full transition-all',
                        resultado.receiaVFTotal >= TETO_MIN ? 'bg-success' : 'bg-danger'
                      )}
                      style={{ width: `${Math.min((resultado.receiaVFTotal / TETO_MAX) * 100, 100)}%` }}
                    />
                  </div>
                </div>

                {/* Alertas */}
                {resultado.alertas.length > 0 && (
                  <div className="space-y-1">
                    {resultado.alertas.map((a, i) => (
                      <p key={i} className="text-warning text-xs flex items-start gap-1">
                        <AlertTriangle className="w-3 h-3 flex-shrink-0 mt-0.5" />
                        {a}
                      </p>
                    ))}
                  </div>
                )}

                {/* Checklist Sancor */}
                {seguradora === 'SANCOR' && (
                  <div className="border-t border-border pt-3">
                    <p className="text-text-faint text-xs font-medium mb-2">Checklist Sancor</p>
                    <div className="space-y-1 text-xs text-text-muted">
                      <Check ok={vg <= 20_000_000} label="Dívida ≤ R$ 20M" />
                      <Check ok={true} label="PL estimado ≥ R$ 20M (confirmar)" />
                      <Check ok={true} label="Lucro Real ou Presumido (confirmar)" />
                      <Check ok={true} label="Consulta cadastral pré-aprovada" />
                    </div>
                  </div>
                )}

                {/* Salvar */}
                <button
                  onClick={salvarProposta}
                  disabled={!cnpjRef.trim() || cnpjRef.length < 8 || saving}
                  className="btn-primary w-full justify-center"
                >
                  {saved
                    ? <><CheckCircle className="w-4 h-4" /> Proposta salva!</>
                    : saving
                    ? 'Salvando…'
                    : <><Save className="w-4 h-4" /> Salvar como rascunho</>
                  }
                </button>
                {!cnpjRef.trim() && (
                  <p className="text-text-faint text-[10px] text-center">
                    Informe o CNPJ Raiz para vincular a proposta à empresa
                  </p>
                )}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

function Check({ ok, label }: { ok: boolean; label: string }) {
  return (
    <div className={cn('flex items-center gap-1.5', ok ? 'text-success' : 'text-text-faint')}>
      {ok ? <CheckCircle className="w-3 h-3" /> : <span className="w-3 h-3 border border-border-strong rounded-full block" />}
      {label}
    </div>
  )
}
