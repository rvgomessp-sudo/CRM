// lib/importador.ts
import Papa from 'papaparse'
import {
  extrairCnpjRaiz,
  calcularPrioridade,
  sugerirSeguradora,
  deveExcluir,
} from './utils'
import type { SupabaseClient } from '@supabase/supabase-js'

interface LinhaCSVF2 {
  CNPJ_RAIZ: string
  CNPJ_COMPLETO: string
  NOME_DEVEDOR: string
  UF_DEVEDOR: string
  QTD_INSCRICOES_EMPRESA: string
  PRIORIDADE_MARINHEIRO: string
  NUMERO_INSCRICAO: string
  SITUACAO_INSCRICAO: string
  TIPO_GARANTIA: string
  FLAG_GARANTIA: string
  TRIBUTO: string
  RECEITA_PRINCIPAL: string
  DATA_INSCRICAO: string
  DIAS_INSCRICAO: string
  ANO_INSCRICAO: string
  VALOR_BRL: string
  VALOR_NUMERICO: string
  INDICADOR_AJUIZADO: string
  UNIDADE_RESPONSAVEL: string
}

export interface ResultadoImportacao {
  empresasNovas: number
  empresasAtualizadas: number
  inscricoesNovas: number
  inscricoesDuplicadas: number
  erros: number
  detalhes: string[]
}

interface DadosEmpresa {
  cnpj_raiz: string
  cnpj_completo: string
  nome_devedor: string
  uf_devedor: string | null
  qtd_inscricoes_empresa: number
  prioridade: string
  estagio: string
  excluido: boolean
  fonte: string
  valor_total_brl: number | null
  valor_minimo_inscricao: number | null
  valor_maximo_inscricao: number | null
  seguradora: string
}

function parseDateBR(dateStr: string): string | null {
  if (!dateStr) return null
  const parts = dateStr.split('/')
  if (parts.length !== 3) return null
  const [day, month, year] = parts
  return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`
}

export async function importarCSVF2(
  file: File,
  supabase: SupabaseClient,
  usuarioId?: string
): Promise<ResultadoImportacao> {
  const resultado: ResultadoImportacao = {
    empresasNovas: 0,
    empresasAtualizadas: 0,
    inscricoesNovas: 0,
    inscricoesDuplicadas: 0,
    erros: 0,
    detalhes: [],
  }

  const texto = await file.text()
  const parsed = await new Promise<Papa.ParseResult<LinhaCSVF2>>((resolve) => {
    Papa.parse<LinhaCSVF2>(texto, {
      header: true,
      delimiter: ';',
      skipEmptyLines: true,
      transformHeader: (h: string) => h.trim().replace(/^\uFEFF/, ''),
      complete: (results) => resolve(results),
    })
  })

  if (parsed.errors.length > 0) {
    resultado.detalhes.push(
      `Erros de parse: ${parsed.errors.slice(0, 5).map((e) => e.message).join(', ')}`
    )
  }

  const linhas = parsed.data
  const empresaMap = new Map<string, { empresa: DadosEmpresa; inscricoes: LinhaCSVF2[] }>()

  for (const linha of linhas) {
    if (!linha.NOME_DEVEDOR) continue

    const cnpjRaiz = extrairCnpjRaiz(linha.CNPJ_RAIZ || linha.CNPJ_COMPLETO || '')
    if (cnpjRaiz.length !== 8) {
      resultado.erros++
      continue
    }

    const { excluir, motivo } = deveExcluir(linha.NOME_DEVEDOR)
    if (excluir) {
      resultado.detalhes.push(`Excluído: ${linha.NOME_DEVEDOR} — ${motivo}`)
      continue
    }

    if (!empresaMap.has(cnpjRaiz)) {
      const qtd = parseInt(linha.QTD_INSCRICOES_EMPRESA) || 1
      empresaMap.set(cnpjRaiz, {
        empresa: {
          cnpj_raiz: cnpjRaiz,
          cnpj_completo: linha.CNPJ_COMPLETO,
          nome_devedor: linha.NOME_DEVEDOR,
          uf_devedor: linha.UF_DEVEDOR || null,
          qtd_inscricoes_empresa: qtd,
          prioridade: calcularPrioridade(qtd),
          estagio: 'base_pgfn',
          excluido: false,
          fonte: 'F2',
          valor_total_brl: null,
          valor_minimo_inscricao: null,
          valor_maximo_inscricao: null,
          seguradora: 'Indefinida',
        },
        inscricoes: [],
      })
    }
    empresaMap.get(cnpjRaiz)!.inscricoes.push(linha)
  }

  const BATCH = 50
  const cnpjsExistentes = new Set<string>()
  const allCnpjs = Array.from(empresaMap.keys())

  for (let i = 0; i < allCnpjs.length; i += BATCH) {
    const lote = allCnpjs.slice(i, i + BATCH)
    const { data } = await supabase.from('empresas').select('cnpj_raiz').in('cnpj_raiz', lote)
    data?.forEach((e: { cnpj_raiz: string }) => cnpjsExistentes.add(e.cnpj_raiz))
  }

  for (const [cnpjRaiz, { empresa, inscricoes }] of empresaMap.entries()) {
    try {
      const valores = inscricoes.map((i) => parseFloat(i.VALOR_NUMERICO) || 0).filter((v) => v > 0)
      const valorTotal = valores.reduce((acc, v) => acc + v, 0)
      empresa.valor_total_brl = valorTotal || null
      empresa.valor_minimo_inscricao = valores.length ? Math.min(...valores) : null
      empresa.valor_maximo_inscricao = valores.length ? Math.max(...valores) : null
      empresa.seguradora = sugerirSeguradora(valorTotal)

      if (cnpjsExistentes.has(cnpjRaiz)) {
        await supabase
          .from('empresas')
          .update({
            cnpj_completo: empresa.cnpj_completo,
            nome_devedor: empresa.nome_devedor,
            qtd_inscricoes_empresa: empresa.qtd_inscricoes_empresa,
            valor_total_brl: empresa.valor_total_brl,
            valor_minimo_inscricao: empresa.valor_minimo_inscricao,
            valor_maximo_inscricao: empresa.valor_maximo_inscricao,
            atualizado_em: new Date().toISOString(),
          })
          .eq('cnpj_raiz', cnpjRaiz)
        resultado.empresasAtualizadas++
      } else {
        await supabase.from('empresas').insert(empresa)
        resultado.empresasNovas++
      }

      for (const insc of inscricoes) {
        const { error } = await supabase.from('inscricoes').upsert(
          {
            cnpj_raiz: cnpjRaiz,
            numero_inscricao: insc.NUMERO_INSCRICAO,
            situacao_inscricao: insc.SITUACAO_INSCRICAO || null,
            tipo_garantia: insc.TIPO_GARANTIA || null,
            flag_garantia: insc.FLAG_GARANTIA || null,
            tributo: insc.TRIBUTO || null,
            receita_principal: insc.RECEITA_PRINCIPAL || null,
            data_inscricao: parseDateBR(insc.DATA_INSCRICAO),
            dias_inscricao: parseInt(insc.DIAS_INSCRICAO) || null,
            ano_inscricao: parseInt(insc.ANO_INSCRICAO) || null,
            valor_brl: insc.VALOR_BRL || null,
            valor_numerico: parseFloat(insc.VALOR_NUMERICO) || null,
            indicador_ajuizado: insc.INDICADOR_AJUIZADO === 'SIM',
            unidade_responsavel: insc.UNIDADE_RESPONSAVEL || null,
          },
          { onConflict: 'numero_inscricao' }
        )
        if (error) {
          if (error.code === '23505') {
            resultado.inscricoesDuplicadas++
          } else {
            resultado.erros++
            resultado.detalhes.push(`Inscrição ${insc.NUMERO_INSCRICAO}: ${error.message}`)
          }
        } else {
          resultado.inscricoesNovas++
        }
      }
    } catch (err) {
      resultado.erros++
      resultado.detalhes.push(`CNPJ ${cnpjRaiz}: ${String(err)}`)
    }
  }

  await supabase.from('log_importacao').insert({
    fonte: 'F2',
    nome_arquivo: file.name,
    total_linhas: linhas.length,
    empresas_novas: resultado.empresasNovas,
    empresas_atualizadas: resultado.empresasAtualizadas,
    inscricoes_novas: resultado.inscricoesNovas,
    inscricoes_duplicadas: resultado.inscricoesDuplicadas,
    erros: resultado.erros,
    detalhes: resultado.detalhes,
    importado_por: usuarioId || null,
  })

  return resultado
}
