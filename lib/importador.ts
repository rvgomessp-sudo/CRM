// lib/importador.ts
import Papa from 'papaparse'
import {
  extrairCnpjRaiz,
  calcularPrioridade,
  sugerirSeguradora,
  deveExcluir,
} from './utils'
import type { SupabaseClient } from '@supabase/supabase-js'

// ── Linha bruta do CSV F2 ─────────────────────────────────
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

  // ── Parse do CSV ──────────────────────────────────────
  const texto = await file.text()
  const parsed = await new Promise<Papa.ParseResult<LinhaCSVF2>>((resolve) => {
    Papa.parse<LinhaCSVF2>(texto, {
      header: true,
      delimiter: ';',
      skipEmptyLines: true,
      transformHeader: (h) => h.trim().replace(/^\uFEFF/, ''),
      complete: (results) => resolve(results),
    })
  })

  if (parsed.errors.length > 0) {
    resultado.detalhes.push(`Erros de parse: ${parsed.errors.slice(0, 5).map(e => e.message).join(', ')}`)
  }

  const linhas = parsed.data

  // ── Agrupar por CNPJ_RAIZ ─────────────────────────────
  const empresaMap = new Map<string, { empresa: Record<string, unknown>, inscricoes: LinhaCSVF2[] }>()

  for (const linha of linhas) {
    if (!linha.NOME_DEVEDOR) continue

    // Garante CNPJ_RAIZ correto (8 dígitos, com zero leading)
    const cnpjRaiz = extrairCnpjRaiz(linha.CNPJ_RAIZ || linha.CNPJ_COMPLETO || '')
    if (cnpjRaiz.length !== 8) {
      resultado.erros++
      continue
    }

    // Verificar exclusão automática
    const { excluir, motivo } = deveExcluir(linha.NOME_DEVEDOR)
    if (excluir) {
      resultado.detalhes.push(`Excluído automaticamente: ${linha.NOME_DEVEDOR} — ${motivo}`)
      continue
    }

    if (!empresaMap.has(cnpjRaiz)) {
      const qtd = parseInt(linha.QTD_INSCRICOES_EMPRESA) || 1
      const prioridade = calcularPrioridade(qtd)
      
      // Valor total será calculado após agregar todas as inscrições
      empresaMap.set(cnpjRaiz, {
        empresa: {
          cnpj_raiz: cnpjRaiz,
          cnpj_completo: linha.CNPJ_COMPLETO,
          nome_devedor: linha.NOME_DEVEDOR,
          uf_devedor: linha.UF_DEVEDOR || null,
          qtd_inscricoes_empresa: qtd,
          prioridade,
          estagio: 'base_pgfn',
          excluido: false,
          fonte: 'F2',
        },
        inscricoes: [],
      })
    }

    empresaMap.get(cnpjRaiz)!.inscricoes.push(linha)
  }

  // ── Upsert em lotes ───────────────────────────────────
  const BATCH = 50
  const cnpjsExistentes = new Set<string>()

  // Checar quais CNPJ_RAIZ já existem
  const allCnpjs = Array.from(empresaMap.keys())
  for (let i = 0; i < allCnpjs.length; i += BATCH) {
    const lote = allCnpjs.slice(i, i + BATCH)
    const { data } = await supabase
      .from('empresas')
      .select('cnpj_raiz')
      .in('cnpj_raiz', lote)
    
    data?.forEach(e => cnpjsExistentes.add(e.cnpj_raiz))
  }

  // Inserir/atualizar empresas
  for (const [cnpjRaiz, { empresa, inscricoes }] of empresaMap.entries()) {
    try {
      // Calcular valor total e min/max das inscrições desta empresa
      const valores = inscricoes.map(i => parseFloat(i.VALOR_NUMERICO) || 0).filter(v => v > 0)
      const valorTotal = valores.reduce((acc, v) => acc + v, 0)
      const valorMin = valores.length ? Math.min(...valores) : null
      const valorMax = valores.length ? Math.max(...valores) : null
      const seguradora = sugerirSeguradora(valorTotal)

      const dadosEmpresa = {
        ...empresa,
        valor_total_brl: valorTotal || null,
        valor_minimo_inscricao: valorMin,
        valor_maximo_inscricao: valorMax,
        seguradora,
      }

      const existe = cnpjsExistentes.has(cnpjRaiz)

      if (existe) {
        // Atualizar apenas campos que NÃO sobrescrevem o pipeline
        await supabase
          .from('empresas')
          .update({
            cnpj_completo: dadosEmpresa.cnpj_completo,
            nome_devedor: dadosEmpresa.nome_devedor,
            qtd_inscricoes_empresa: dadosEmpresa.qtd_inscricoes_empresa,
            valor_total_brl: dadosEmpresa.valor_total_brl,
            valor_minimo_inscricao: dadosEmpresa.valor_minimo_inscricao,
            valor_maximo_inscricao: dadosEmpresa.valor_maximo_inscricao,
            atualizado_em: new Date().toISOString(),
          })
          .eq('cnpj_raiz', cnpjRaiz)
        resultado.empresasAtualizadas++
      } else {
        await supabase.from('empresas').insert(dadosEmpresa)
        resultado.empresasNovas++
      }

      // Inscrições — dedup por numero_inscricao
      for (const insc of inscricoes) {
        const { error } = await supabase
          .from('inscricoes')
          .upsert(
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

  // ── Log de importação ─────────────────────────────────
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

// ── Helper: converter data BR (DD/MM/YYYY) para ISO ───────
function parseDateBR(dateStr: string): string | null {
  if (!dateStr) return null
  const parts = dateStr.split('/')
  if (parts.length !== 3) return null
  const [day, month, year] = parts
  return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`
}
