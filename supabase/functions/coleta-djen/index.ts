// ============================================================
// COLETA DJEN — Edge Function (Fase 15 · coleta autorizada)
// A cada execução: pega o lote de processos menos recentemente
// checados, consulta a API pública Comunica/CNJ, insere só o que é
// NOVO (dedup por numero_comunicacao), classifica tipo/ramo, gera
// alerta CANDIDATO para constrição, e registra o log em `coletas`.
// Fonte pública, rate limit respeitado (350ms entre chamadas),
// sem contornar autenticação ou captcha.
// ============================================================
import { createClient } from 'jsr:@supabase/supabase-js@2'

const TIPOS: [RegExp, string][] = [
  [/sisbajud|bacenjud|bloqueio de valores|indisponibilidade de ativos/i, 'SISBAJUD'],
  [/renajud/i, 'RENAJUD'],
  [/penhora/i, 'PENHORA'],
  [/embargos [àa] execu/i, 'EMBARGOS_EXEC'],
  [/mandado de seguran/i, 'MANDADO_SEGURANCA'],
  [/a[çc][ãa]o anulat[óo]ria/i, 'ANULATORIA'],
  [/cautelar|tutela de urg[êe]ncia/i, 'CAUTELAR'],
  [/execu[çc][ãa]o fiscal/i, 'EXECUCAO_FISCAL'],
]
const CONSTRICAO = new Set(['SISBAJUD', 'PENHORA', 'RENAJUD'])

function classificar(texto: string): string {
  for (const [re, tipo] of TIPOS) if (re.test(texto)) return tipo
  return 'PUBLICACAO' // sem peso no score; fica registrado como fato
}
function ramoDe(proc: string): string {
  const j = proc[13]
  return j === '4' ? 'FEDERAL' : j === '5' ? 'TRABALHISTA' : j === '8' ? 'ESTADUAL' : 'OUTRO'
}
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms))

Deno.serve(async (req) => {
  const supa = createClient(
    Deno.env.get('SUPABASE_URL')!,
    Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!,
  )
  const { batch = 40 } = await req.json().catch(() => ({}))
  const inicio = new Date().toISOString()
  let processados = 0, novosEventos = 0, novosAlertas = 0, erros = 0
  let detalhe = ''

  const { data: fila, error: eFila } = await supa
    .from('processos_monitorados').select('numero_processo, cnpj_raiz')
    .eq('ativo', true)
    .order('ultima_checagem', { ascending: true, nullsFirst: true })
    .limit(Math.min(batch, 60))
  if (eFila || !fila?.length) {
    await supa.from('coletas').insert({ fonte: 'DJEN_COMUNICA', concluida_em: new Date().toISOString(), detalhe: eFila?.message ?? 'fila vazia' })
    return new Response(JSON.stringify({ ok: true, fila: 0 }), { headers: { 'Content-Type': 'application/json' } })
  }

  for (const p of fila) {
    try {
      const r = await fetch(
        `https://comunicaapi.pje.jus.br/api/v1/comunicacao?numeroProcesso=${p.numero_processo}&pagina=1&tamanhoPagina=20`,
        { headers: { 'User-Agent': 'VF-CRM-monitor/1.0' }, signal: AbortSignal.timeout(12000) },
      )
      if (r.status === 429) { detalhe = 'rate limit 429 — lote interrompido'; break }
      processados++
      if (!r.ok || !(r.headers.get('content-type') ?? '').includes('json')) { erros++; continue }
      const j = await r.json()
      const items: any[] = j.items ?? []

      // dedup: comunicações já registradas desta EMPRESA (conjunto pequeno,
      // cobre também as gravadas pelo enriquecimento manual de 18/08);
      // segunda barreira: UNIQUE(hash) em eventos
      const { data: exist } = await supa.from('eventos')
        .select('numero_comunicacao').eq('fonte', 'JUDICIAL')
        .eq('cnpj_raiz', p.cnpj_raiz).not('numero_comunicacao', 'is', null)
      const vistos = new Set((exist ?? []).map(e => String(e.numero_comunicacao)))

      let novidades = 0
      for (const it of items) {
        const nc = it.numeroComunicacao ?? it.numero_comunicacao
        if (!nc || vistos.has(String(nc))) continue
        const texto = String(it.texto ?? '').replace(/\s+/g, ' ').trim().slice(0, 1200)
        const tipo = classificar(texto)
        const advMap = new Map<string, unknown>()
        for (const da of it.destinatarioadvogados ?? []) {
          const a = da?.advogado
          if (a?.nome) advMap.set(`${a.nome}|${a.numero_oab}`, { nome: a.nome, oab: a.numero_oab ?? null, uf: a.uf_oab ?? null })
        }
        const advs = [...advMap.values()].slice(0, 1) // regra: um advogado (primeiro coletado)
        const ocorrido = (it.data_disponibilizacao ?? it.datadisponibilizacao ?? inicio).slice(0, 10)

        const { error: eIns } = await supa.from('eventos').insert({
          fonte: 'JUDICIAL', tipo,
          cnpj_raiz: p.cnpj_raiz,
          numero_processo: p.numero_processo,
          ocorrido_em: ocorrido,
          capturado_em: new Date().toISOString(),
          payload: { ramo: ramoDe(p.numero_processo), origem: 'coleta-djen' },
          hash: `dj|${p.numero_processo}|${nc}`,
          texto: texto || null,
          advogados: advs.length ? advs : null,
          numero_comunicacao: Number(nc) || null,
          link_publicacao: it.link ?? null,
          enriquecido_em: new Date().toISOString(),
        })
        if (eIns) { if (!/duplicate|unique/i.test(eIns.message)) erros++; continue }
        novidades++; novosEventos++

        if (CONSTRICAO.has(tipo) && ramoDe(p.numero_processo) !== 'TRABALHISTA') {
          const { error: eAl } = await supa.from('alertas').insert({
            cnpj_raiz: p.cnpj_raiz, numero_processo: p.numero_processo,
            titulo: `Coleta DJEN · ${tipo} detectado`,
            gravidade: 'CRITICO', estado: 'CANDIDATO',
            evidencia_condicao: 'CORROBORADO', papel_processual: 'NAO_CONFIRMADO',
            ramo: ramoDe(p.numero_processo),
            pendencias: ['CONFERIR_PAPEL_PROCESSUAL', 'CONFERIR_ATRIBUICAO_LOCAL'],
            fonte: 'DJEN_COMUNICA', link_fonte: it.link ?? null, trecho: texto.slice(0, 400),
          })
          if (!eAl) novosAlertas++
        }
      }

      await supa.rpc('fn_incrementa_checagem', { p_processo: p.numero_processo, p_novidades: novidades })
      await sleep(350)
    } catch {
      erros++
    }
  }

  await supa.from('coletas').insert({
    fonte: 'DJEN_COMUNICA', iniciada_em: inicio, concluida_em: new Date().toISOString(),
    processados, novos_eventos: novosEventos, novos_alertas: novosAlertas, erros,
    detalhe: detalhe || null,
  })
  return new Response(JSON.stringify({ ok: true, processados, novosEventos, novosAlertas, erros }),
    { headers: { 'Content-Type': 'application/json' } })
})
