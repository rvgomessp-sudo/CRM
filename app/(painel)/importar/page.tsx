'use client'
// app/(painel)/importar/page.tsx
import { useState, useRef } from 'react'
import { createClient } from '@/lib/supabase/client'
import { importarCSVF2, ResultadoImportacao } from '@/lib/importador'

type Status = 'idle' | 'loading' | 'done' | 'error'

export default function ImportarPage() {
  const [status, setStatus] = useState<Status>('idle')
  const [resultado, setResultado] = useState<ResultadoImportacao | null>(null)
  const [erro, setErro] = useState('')
  const [drag, setDrag] = useState(false)
  const [nomeArquivo, setNomeArquivo] = useState('')
  const inputRef = useRef<HTMLInputElement>(null)
  const supabase = createClient()

  async function processarArquivo(file: File) {
    if (!file.name.endsWith('.csv')) {
      setErro('Apenas arquivos CSV são aceitos.')
      return
    }
    setNomeArquivo(file.name)
    setStatus('loading')
    setErro('')
    try {
      const { data: { user } } = await supabase.auth.getUser()
      const res = await importarCSVF2(file, supabase, user?.id)
      setResultado(res)
      setStatus('done')
    } catch (err) {
      setErro(String(err))
      setStatus('error')
    }
  }

  function onDrop(e: React.DragEvent) {
    e.preventDefault()
    setDrag(false)
    const file = e.dataTransfer.files[0]
    if (file) processarArquivo(file)
  }

  function onFileChange(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0]
    if (file) processarArquivo(file)
  }

  function resetar() {
    setStatus('idle')
    setResultado(null)
    setErro('')
    setNomeArquivo('')
    if (inputRef.current) inputRef.current.value = ''
  }

  return (
    <div style={{ maxWidth: 700 }}>
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 20, fontWeight: 700, marginBottom: 4 }}>Importar Base PGFN</h1>
        <p style={{ color: 'var(--color-text-muted)', fontSize: 13 }}>
          Importação inteligente com deduplicação por CNPJ_RAIZ. O pipeline existente é preservado.
        </p>
      </div>

      {/* Instruções */}
      <div className="card" style={{ marginBottom: 20, background: 'rgba(59,130,246,0.05)', borderColor: 'rgba(59,130,246,0.2)' }}>
        <h3 style={{ fontSize: 13, fontWeight: 600, marginBottom: 10, color: '#60a5fa' }}>Formato esperado — CSV F2</h3>
        <div style={{ fontSize: 12, color: 'var(--color-text-muted)', lineHeight: 1.8 }}>
          <div>• Separador: <code style={{ background: 'var(--color-surface-2)', padding: '1px 6px', borderRadius: 3 }}>;</code></div>
          <div>• Encoding: UTF-8 com BOM (UTF-8-sig)</div>
          <div>• Arquivo: <code style={{ background: 'var(--color-surface-2)', padding: '1px 6px', borderRadius: 3 }}>01_f2_inscricoes_individuais.csv</code></div>
          <div>• Exclusões automáticas: Massa Falida, Recuperação Judicial, Falido, Simples/MEI</div>
          <div>• Duplicatas: identificadas por NUMERO_INSCRICAO — atualizadas sem sobrescrever o pipeline</div>
        </div>
      </div>

      {/* Drop zone */}
      {status === 'idle' && (
        <div
          onDragOver={e => { e.preventDefault(); setDrag(true) }}
          onDragLeave={() => setDrag(false)}
          onDrop={onDrop}
          onClick={() => inputRef.current?.click()}
          style={{
            border: `2px dashed ${drag ? '#3b82f6' : 'var(--color-border)'}`,
            borderRadius: 10,
            padding: 48,
            textAlign: 'center',
            cursor: 'pointer',
            background: drag ? 'rgba(59,130,246,0.05)' : 'var(--color-surface)',
            transition: 'all 0.2s',
            marginBottom: 20,
          }}
        >
          <div style={{ fontSize: 36, marginBottom: 12 }}>↑</div>
          <div style={{ fontSize: 15, fontWeight: 600, marginBottom: 6 }}>Arraste o CSV aqui</div>
          <div style={{ fontSize: 13, color: 'var(--color-text-muted)', marginBottom: 16 }}>
            ou clique para selecionar o arquivo
          </div>
          <div style={{
            display: 'inline-block',
            padding: '8px 20px',
            background: 'var(--color-primary)',
            color: 'white',
            borderRadius: 6,
            fontSize: 13,
            fontWeight: 500,
          }}>
            Selecionar arquivo CSV
          </div>
          <input
            ref={inputRef}
            type="file"
            accept=".csv"
            onChange={onFileChange}
            style={{ display: 'none' }}
          />
        </div>
      )}

      {/* Loading */}
      {status === 'loading' && (
        <div className="card" style={{ textAlign: 'center', padding: 48 }}>
          <div style={{ fontSize: 13, color: 'var(--color-text-muted)', marginBottom: 8 }}>
            Processando {nomeArquivo}...
          </div>
          <div style={{ fontSize: 12, color: 'var(--color-text-muted)' }}>
            Deduplicando, inserindo empresas e inscrições no Supabase
          </div>
          <div style={{
            marginTop: 20,
            height: 4,
            background: 'var(--color-border)',
            borderRadius: 2,
            overflow: 'hidden',
          }}>
            <div style={{
              height: '100%',
              background: 'var(--color-primary)',
              borderRadius: 2,
              animation: 'pulse 1.5s ease-in-out infinite',
              width: '60%',
            }} />
          </div>
        </div>
      )}

      {/* Resultado */}
      {status === 'done' && resultado && (
        <div>
          <div className="card" style={{
            marginBottom: 16,
            background: 'rgba(16,185,129,0.05)',
            borderColor: 'rgba(16,185,129,0.2)',
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 16 }}>
              <span style={{ fontSize: 20 }}>✓</span>
              <div>
                <div style={{ fontSize: 15, fontWeight: 600, color: '#10b981' }}>Importação concluída</div>
                <div style={{ fontSize: 12, color: 'var(--color-text-muted)' }}>{nomeArquivo}</div>
              </div>
            </div>

            {/* KPIs do resultado */}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: 10 }}>
              {[
                { label: 'Empresas novas', value: resultado.empresasNovas, cor: '#10b981' },
                { label: 'Empresas atualizadas', value: resultado.empresasAtualizadas, cor: '#60a5fa' },
                { label: 'Inscrições novas', value: resultado.inscricoesNovas, cor: '#10b981' },
                { label: 'Duplicatas ignoradas', value: resultado.inscricoesDuplicadas, cor: '#fbbf24' },
                { label: 'Erros', value: resultado.erros, cor: resultado.erros > 0 ? '#f87171' : 'var(--color-text-muted)' },
              ].map(k => (
                <div key={k.label} style={{
                  background: 'var(--color-surface-2)',
                  borderRadius: 6,
                  padding: '10px 12px',
                  textAlign: 'center',
                }}>
                  <div style={{ fontSize: 22, fontWeight: 800, color: k.cor }}>{k.value}</div>
                  <div style={{ fontSize: 10, color: 'var(--color-text-muted)', marginTop: 2 }}>{k.label}</div>
                </div>
              ))}
            </div>
          </div>

          {/* Detalhes / logs */}
          {resultado.detalhes.length > 0 && (
            <div className="card" style={{ marginBottom: 16 }}>
              <h3 style={{ fontSize: 13, fontWeight: 600, marginBottom: 10, color: 'var(--color-text-muted)' }}>
                LOG DE DETALHES ({resultado.detalhes.length})
              </h3>
              <div style={{
                maxHeight: 200,
                overflowY: 'auto',
                fontFamily: 'monospace',
                fontSize: 11,
                lineHeight: 1.8,
                color: 'var(--color-text-muted)',
              }}>
                {resultado.detalhes.map((d, i) => (
                  <div key={i} style={{ borderBottom: '1px solid var(--color-border)', paddingBottom: 2, marginBottom: 2 }}>
                    {d}
                  </div>
                ))}
              </div>
            </div>
          )}

          <div style={{ display: 'flex', gap: 10 }}>
            <button className="btn btn-ghost" onClick={resetar}>Importar outro arquivo</button>
            <a href="/base" className="btn btn-primary" style={{ textDecoration: 'none' }}>
              Ver Base PGFN →
            </a>
          </div>
        </div>
      )}

      {/* Erro */}
      {status === 'error' && (
        <div className="card" style={{ borderColor: 'rgba(239,68,68,0.2)', background: 'rgba(239,68,68,0.05)' }}>
          <div style={{ color: '#f87171', fontSize: 14, fontWeight: 600, marginBottom: 8 }}>Erro na importação</div>
          <div style={{ fontFamily: 'monospace', fontSize: 12, color: 'var(--color-text-muted)' }}>{erro}</div>
          <button className="btn btn-ghost" onClick={resetar} style={{ marginTop: 16 }}>Tentar novamente</button>
        </div>
      )}

      {/* Histórico de importações */}
      <HistoricoImportacoes />
    </div>
  )
}

function HistoricoImportacoes() {
  const [logs, setLogs] = useState<Record<string, string | number>[]>([])
  const supabase = createClient()

  useState(() => {
    supabase
      .from('log_importacao')
      .select('*')
      .order('criado_em', { ascending: false })
      .limit(10)
      .then(({ data }) => setLogs(data || []))
  })

  if (logs.length === 0) return null

  return (
    <div className="card" style={{ marginTop: 24 }}>
      <h3 style={{ fontSize: 13, fontWeight: 600, marginBottom: 12, color: 'var(--color-text-muted)' }}>
        HISTÓRICO DE IMPORTAÇÕES
      </h3>
      <table>
        <thead>
          <tr>
            <th>Arquivo</th>
            <th>Fonte</th>
            <th>Empresas novas</th>
            <th>Inscrições</th>
            <th>Erros</th>
            <th>Data</th>
          </tr>
        </thead>
        <tbody>
          {logs.map(log => (
            <tr key={log.id as string}>
              <td style={{ fontFamily: 'monospace', fontSize: 11 }}>{log.nome_arquivo}</td>
              <td><span style={{ padding: '2px 8px', background: 'rgba(59,130,246,0.1)', color: '#60a5fa', borderRadius: 10, fontSize: 11 }}>{log.fonte}</span></td>
              <td>{log.empresas_novas}</td>
              <td>{log.inscricoes_novas}</td>
              <td style={{ color: Number(log.erros) > 0 ? '#f87171' : 'var(--color-text-muted)' }}>{log.erros}</td>
              <td style={{ fontSize: 11, color: 'var(--color-text-muted)' }}>
                {new Date(log.criado_em as string).toLocaleString('pt-BR')}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
