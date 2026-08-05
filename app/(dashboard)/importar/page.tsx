'use client'

import { useState, useRef } from 'react'
import Papa from 'papaparse'
import { createClient } from '@/lib/supabase/client'
import {
  extractCnpjRaiz, parseBRLString, parseCSVDate,
  mapPrioridade, mapFaixa, mapSeguradora, formatBRL
} from '@/lib/utils'
import { classificarMotor, motorMaisUrgente } from '@/lib/motor'
import type { CSVRow, MotorTipo, PrioridadeTipo } from '@/lib/types'
import { Upload, AlertTriangle, CheckCircle, FileText, Trash2, Loader2 } from 'lucide-react'

interface ImportResult {
  total: number
  importadas: number
  atualizadas: number
  ignoradas: number
  erros: string[]
}

interface ParsedRow {
  cnpjRaiz: string
  cnpjCompleto: string
  nomeDevedor: string
  uf: string
  prioridade: PrioridadeTipo
  numeroInscricao: string
  situacaoInscricao: string
  tipoGarantia: string
  flagGarantia: string
  tributo: string
  receitaPrincipal: string
  dataInscricao: string | null
  diasInscricao: number
  anoInscricao: number
  valorBrl: string
  valorNumerico: number
  ajuizado: boolean
  unidadeResponsavel: string
}

const BATCH_SIZE = 20  // upserts por rodada

export default function ImportarPage() {
  const supabase = createClient()
  const fileRef = useRef<HTMLInputElement>(null)

  const [file, setFile] = useState<File | null>(null)
  const [preview, setPreview] = useState<CSVRow[]>([])
  const [parsedRows, setParsedRows] = useState<ParsedRow[]>([])
  const [parsing, setParsing] = useState(false)
  const [importing, setImporting] = useState(false)
  const [progress, setProgress] = useState(0)
  const [result, setResult] = useState<ImportResult | null>(null)
  const [existingCount, setExistingCount] = useState<number | null>(null)

  function handleFileSelect(e: React.ChangeEvent<HTMLInputElement>) {
    const f = e.target.files?.[0]
    if (!f) return
    setFile(f)
    setResult(null)
    setParsedRows([])
    setPreview([])
    setExistingCount(null)
    parseCSV(f)
  }

  function parseCSV(f: File) {
    setParsing(true)
    Papa.parse<CSVRow>(f, {
      header: true,
      delimiter: ';',
      encoding: 'UTF-8',
      skipEmptyLines: true,
      complete: async (results) => {
        const rawRows = results.data as CSVRow[]
        setPreview(rawRows.slice(0, 5))

        // Processa todas as linhas
        const processed: ParsedRow[] = rawRows.map(row => ({
          cnpjRaiz:        extractCnpjRaiz(row.CNPJ_COMPLETO || row.CNPJ_RAIZ || ''),
          cnpjCompleto:    row.CNPJ_COMPLETO || '',
          nomeDevedor:     row.NOME_DEVEDOR || '',
          uf:              row.UF_DEVEDOR || '',
          prioridade:      mapPrioridade(row.PRIORIDADE_MARINHEIRO || ''),
          numeroInscricao: row.NUMERO_INSCRICAO || '',
          situacaoInscricao: row.SITUACAO_INSCRICAO || '',
          tipoGarantia:    row.TIPO_GARANTIA || '',
          flagGarantia:    row.FLAG_GARANTIA || '',
          tributo:         row.TRIBUTO || '',
          receitaPrincipal: row.RECEITA_PRINCIPAL || '',
          dataInscricao:   parseCSVDate(row.DATA_INSCRICAO || ''),
          diasInscricao:   parseInt(row.DIAS_INSCRICAO || '0') || 0,
          anoInscricao:    parseInt(row.ANO_INSCRICAO || '0') || 0,
          valorBrl:        row.VALOR_BRL || '',
          valorNumerico:   parseBRLString(row.VALOR_NUMERICO || row.VALOR_BRL || '0'),
          ajuizado:        (row.INDICADOR_AJUIZADO || '').toUpperCase() === 'SIM',
          unidadeResponsavel: row.UNIDADE_RESPONSAVEL || '',
        }))

        setParsedRows(processed)

        // Verifica se já tem dados no banco
        const { count } = await supabase
          .from('empresas')
          .select('*', { count: 'exact', head: true })
        setExistingCount(count || 0)

        setParsing(false)
      },
      error: (err) => {
        console.error('Parse error:', err)
        setParsing(false)
      }
    })
  }

  async function handleImport() {
    if (parsedRows.length === 0) return
    setImporting(true)
    setProgress(0)

    const resultado: ImportResult = {
      total: parsedRows.length,
      importadas: 0,
      atualizadas: 0,
      ignoradas: 0,
      erros: [],
    }

    // Agrupa por CNPJ_RAIZ
    const byEmpresa = new Map<string, ParsedRow[]>()
    for (const row of parsedRows) {
      if (!row.cnpjRaiz || row.cnpjRaiz.length !== 8) {
        resultado.ignoradas++
        continue
      }
      if (!byEmpresa.has(row.cnpjRaiz)) byEmpresa.set(row.cnpjRaiz, [])
      byEmpresa.get(row.cnpjRaiz)!.push(row)
    }

    const empresaList = Array.from(byEmpresa.entries())
    const total = empresaList.length

    for (let i = 0; i < empresaList.length; i += BATCH_SIZE) {
      const batch = empresaList.slice(i, i + BATCH_SIZE)

      for (const [cnpjRaiz, rows] of batch) {
        try {
          // Calcula motor por inscrição
          const motoresInscricoes = rows.map(r =>
            classificarMotor(r.situacaoInscricao, r.tipoGarantia, r.ajuizado)
          )
          const motorEmpresa = motorMaisUrgente(motoresInscricoes)

          const valorTotal = rows.reduce((s, r) => s + r.valorNumerico, 0)
          const valorMaior = Math.max(...rows.map(r => r.valorNumerico))
          const primeiraRow = rows[0]

          // Upsert empresa
          const { error: empErr } = await supabase
            .from('empresas')
            .upsert({
              cnpj_raiz:            cnpjRaiz,
              cnpj_completo:        primeiraRow.cnpjCompleto,
              nome_devedor:         primeiraRow.nomeDevedor,
              uf_devedor:           primeiraRow.uf,
              qtd_inscricoes:       rows.length,
              prioridade:           primeiraRow.prioridade,
              motor:                motorEmpresa,
              faixa:                mapFaixa(valorTotal),
              valor_total_devida:   valorTotal,
              valor_maior_inscricao: valorMaior,
              seguradora_alvo:      mapSeguradora(valorTotal),
              importado_em:         new Date().toISOString(),
              atualizado_em:        new Date().toISOString(),
            }, {
              onConflict: 'cnpj_raiz',
              ignoreDuplicates: false,  // atualiza se já existe
            })

          if (empErr) {
            resultado.erros.push(`Empresa ${cnpjRaiz}: ${empErr.message}`)
            continue
          }

          // Upsert inscrições individuais
          const inscricoesPayload = rows.map((r, idx) => ({
            cnpj_raiz:            cnpjRaiz,
            cnpj_completo:        r.cnpjCompleto,
            nome_devedor:         r.nomeDevedor,
            uf_devedor:           r.uf,
            numero_inscricao:     r.numeroInscricao,
            situacao_inscricao:   r.situacaoInscricao,
            tipo_garantia:        r.tipoGarantia,
            flag_garantia:        r.flagGarantia,
            tributo:              r.tributo,
            receita_principal:    r.receitaPrincipal,
            data_inscricao:       r.dataInscricao,
            dias_inscricao:       r.diasInscricao,
            ano_inscricao:        r.anoInscricao,
            valor_brl:            r.valorBrl,
            valor_numerico:       r.valorNumerico,
            indicador_ajuizado:   r.ajuizado,
            unidade_responsavel:  r.unidadeResponsavel,
            motor:                motoresInscricoes[idx],
            prioridade:           r.prioridade,
          }))

          const { error: insErr } = await supabase
            .from('inscricoes')
            .upsert(inscricoesPayload, {
              onConflict: 'numero_inscricao',
              ignoreDuplicates: false,
            })

          if (insErr) {
            resultado.erros.push(`Inscrições ${cnpjRaiz}: ${insErr.message}`)
          } else {
            resultado.importadas++
          }

        } catch (err: any) {
          resultado.erros.push(`${cnpjRaiz}: ${err.message}`)
        }
      }

      setProgress(Math.round(((i + BATCH_SIZE) / total) * 100))
      // Pausa breve para não sobrecarregar
      await new Promise(r => setTimeout(r, 50))
    }

    setProgress(100)
    setResult(resultado)
    setImporting(false)
  }

  // Agrupa por empresa para o preview
  const empresasCount = new Set(parsedRows.map(r => r.cnpjRaiz)).size

  return (
    <div className="p-6 max-w-4xl mx-auto">
      <h1 className="text-xl font-bold text-text-primary mb-1">Importar Base PGFN</h1>
      <p className="text-text-muted text-sm mb-6">
        CSV F1/F2 — Separador <code className="bg-bg-card px-1 rounded text-xs">;</code> | Encoding UTF-8 BOM
      </p>

      {/* Aviso base existente */}
      {existingCount !== null && existingCount > 0 && (
        <div className="mb-4 p-3 rounded border border-warning/30 bg-warning/10 flex items-center gap-2 text-sm text-warning">
          <AlertTriangle className="w-4 h-4 flex-shrink-0" />
          <span>
            <strong>Atenção:</strong> o banco já contém <strong>{existingCount}</strong> empresas.
            A importação fará UPSERT — atualiza existentes, cria novas. Inscrições são deduplicadas por nº de inscrição.
          </span>
        </div>
      )}

      {/* Upload zone */}
      <div
        className="border-2 border-dashed border-border hover:border-vf-red rounded-lg p-8 text-center cursor-pointer transition-colors mb-6"
        onClick={() => fileRef.current?.click()}
      >
        <Upload className="w-8 h-8 mx-auto mb-2 text-text-faint" />
        {file ? (
          <div>
            <p className="text-text-primary font-medium">{file.name}</p>
            <p className="text-text-muted text-sm">{(file.size / 1024).toFixed(0)} KB</p>
          </div>
        ) : (
          <>
            <p className="text-text-primary font-medium">Selecione o CSV da base PGFN</p>
            <p className="text-text-muted text-sm mt-1">01_f2_inscricoes_individuais.csv ou equivalente</p>
          </>
        )}
        <input
          ref={fileRef}
          type="file"
          accept=".csv"
          className="hidden"
          onChange={handleFileSelect}
        />
      </div>

      {/* Parsing */}
      {parsing && (
        <div className="card flex items-center gap-3 mb-4 text-text-muted">
          <Loader2 className="w-4 h-4 animate-spin text-vf-red" />
          Analisando CSV…
        </div>
      )}

      {/* Preview */}
      {parsedRows.length > 0 && !parsing && (
        <div className="card mb-4">
          <div className="flex items-center justify-between mb-3">
            <p className="section-header mb-0">Preview — Dados detectados</p>
          </div>

          <div className="grid grid-cols-3 gap-4 mb-4">
            <Stat label="Linhas no CSV" value={parsedRows.length} />
            <Stat label="Empresas únicas" value={empresasCount} />
            <Stat label="Total dívida" value={formatBRL(parsedRows.reduce((s, r) => s + r.valorNumerico, 0))} />
          </div>

          {/* Preview table */}
          <p className="text-text-faint text-xs mb-2">Primeiras 5 linhas:</p>
          <div className="overflow-x-auto">
            <table className="table-vf text-[11px]">
              <thead>
                <tr>
                  <th>CNPJ Raiz</th>
                  <th>Empresa</th>
                  <th>Tributo</th>
                  <th>Situação</th>
                  <th>Motor</th>
                  <th className="text-right">Valor</th>
                </tr>
              </thead>
              <tbody>
                {parsedRows.slice(0, 5).map((r, i) => (
                  <tr key={i}>
                    <td className="font-mono">{r.cnpjRaiz}</td>
                    <td className="truncate max-w-36">{r.nomeDevedor}</td>
                    <td>{r.tributo}</td>
                    <td className="truncate max-w-32">{r.situacaoInscricao}</td>
                    <td>{classificarMotor(r.situacaoInscricao, r.tipoGarantia, r.ajuizado) || '—'}</td>
                    <td className="text-right font-semibold">{formatBRL(r.valorNumerico)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Botão importar */}
      {parsedRows.length > 0 && !importing && !result && (
        <button onClick={handleImport} className="btn-primary">
          <Upload className="w-4 h-4" />
          Importar {empresasCount} empresas / {parsedRows.length} inscrições
        </button>
      )}

      {/* Progress */}
      {importing && (
        <div className="card">
          <div className="flex items-center gap-3 mb-3">
            <Loader2 className="w-4 h-4 animate-spin text-vf-red" />
            <span className="text-text-primary text-sm">Importando… {progress}%</span>
          </div>
          <div className="bg-bg-secondary rounded-full h-2 overflow-hidden">
            <div
              className="h-full bg-vf-red rounded-full transition-all duration-300"
              style={{ width: `${progress}%` }}
            />
          </div>
          <p className="text-text-faint text-xs mt-2">
            Não feche esta página. Processando em lotes de {BATCH_SIZE}.
          </p>
        </div>
      )}

      {/* Resultado */}
      {result && (
        <div className="card">
          <div className="flex items-center gap-2 mb-4">
            <CheckCircle className="w-5 h-5 text-success" />
            <p className="text-text-primary font-semibold">Importação concluída</p>
          </div>

          <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-4">
            <Stat label="Total linhas" value={result.total} />
            <Stat label="Empresas criadas/atualizadas" value={result.importadas} color="text-success" />
            <Stat label="Ignoradas" value={result.ignoradas} color="text-warning" />
            <Stat label="Erros" value={result.erros.length} color={result.erros.length > 0 ? 'text-danger' : 'text-success'} />
          </div>

          {result.erros.length > 0 && (
            <div>
              <p className="text-danger text-sm font-medium mb-2">Erros ({result.erros.length}):</p>
              <div className="bg-danger/10 border border-danger/30 rounded p-3 space-y-1 max-h-40 overflow-y-auto">
                {result.erros.slice(0, 20).map((err, i) => (
                  <p key={i} className="text-danger text-xs font-mono">{err}</p>
                ))}
                {result.erros.length > 20 && (
                  <p className="text-danger text-xs">…e mais {result.erros.length - 20} erros</p>
                )}
              </div>
            </div>
          )}

          <div className="flex gap-3 mt-4">
            <a href="/base-pgfn" className="btn-primary text-sm">
              <FileText className="w-4 h-4" /> Ver Base PGFN
            </a>
            <button
              onClick={() => { setFile(null); setResult(null); setParsedRows([]); setPreview([]); if (fileRef.current) fileRef.current.value = '' }}
              className="btn-secondary text-sm"
            >
              <Trash2 className="w-4 h-4" /> Nova importação
            </button>
          </div>
        </div>
      )}
    </div>
  )
}

function Stat({ label, value, color }: { label: string; value: string | number; color?: string }) {
  return (
    <div>
      <p className="text-text-faint text-xs">{label}</p>
      <p className={`text-xl font-bold mt-0.5 ${color || 'text-text-primary'}`}>{value}</p>
    </div>
  )
}
