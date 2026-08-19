// ============================================================
// GERAR DOSSIÊ — enriquecimento por fonte pública (Receita)
// Dado um cnpj_raiz: busca o CNPJ completo (inscricoes), consulta a base
// pública do CNPJ (minhareceita.org), e cria — para VALIDAÇÃO HUMANA:
//   • decisores a partir do QSA (sócios/administradores), como
//     PATROCINADOR_INTERNO_POTENCIAL, poder POTENCIAL, evidência CORROBORADO
//   • o contato cadastral (telefone/e-mail) da empresa em `contatos`
//   • trilha em entity_evidence (entity_type PERSON, fonte RECEITA_QSA)
// Não inventa: só grava o que a Receita retorna. Sem QSA → sem decisor.
// A camada OSINT profunda (Tavily → CFO/jurídico na web) é upgrade futuro.
// ============================================================
import { createClient } from 'jsr:@supabase/supabase-js@2'

Deno.serve(async (req) => {
  const supa = createClient(Deno.env.get('SUPABASE_URL')!, Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!)
  const { cnpj_raiz } = await req.json().catch(() => ({}))
  if (!cnpj_raiz) return json({ ok: false, erro: 'cnpj_raiz obrigatório' }, 400)

  // 1) CNPJ completo a partir das inscrições
  const { data: ins } = await supa.from('inscricoes').select('cnpj_completo')
    .eq('cnpj_raiz', cnpj_raiz).not('cnpj_completo', 'is', null).limit(1)
  const cnpj = (ins?.[0]?.cnpj_completo ?? '').replace(/\D/g, '')
  if (cnpj.length !== 14) return json({ ok: false, erro: 'CNPJ completo não encontrado para esta empresa' }, 404)

  // 2) Base pública do CNPJ
  let j: any = null
  try {
    const r = await fetch(`https://minhareceita.org/${cnpj}`, { signal: AbortSignal.timeout(15000) })
    if (r.ok) j = await r.json()
  } catch { /* ignore */ }
  if (!j) return json({ ok: false, erro: 'Receita indisponível ou CNPJ não encontrado' }, 502)

  let decisores = 0, evidencias = 0
  const nowIso = new Date().toISOString()
  const qsa: any[] = j.qsa ?? []

  for (const s of qsa) {
    const nome = (s.nome_socio ?? s.nome ?? '').trim()
    if (!nome) continue
    const cargo = (s.qualificacao_socio ?? s.qualificacao ?? 'Sócio/Administrador').toString().slice(0, 120)
    const { error: eDec } = await supa.from('decisores').upsert({
      cnpj_raiz, nome, cargo,
      classificacao: 'PATROCINADOR_INTERNO_POTENCIAL',
      poder_decisorio: 'POTENCIAL',
      prioridade: 3,
      fonte: 'RECEITA_QSA',
      evidencia_condicao: 'CORROBORADO',
      trecho: `Quadro de sócios e administradores (Receita) · ${cargo}`,
      link_fonte: `https://minhareceita.org/${cnpj}`,
    }, { onConflict: 'cnpj_raiz,nome,cargo', ignoreDuplicates: false })
    if (!eDec) {
      decisores++
      await supa.from('entity_evidence').insert({
        entity_type: 'PERSON', entity_ref: `${cnpj_raiz}|${nome}`,
        source: 'RECEITA_QSA', link_fonte: `https://minhareceita.org/${cnpj}`,
        trecho: `QSA: ${nome} — ${cargo}`, confidence: 90, condicao: 'CORROBORADO',
        data_captura: nowIso,
      })
      evidencias++
    }
  }

  // 3) Contato cadastral da empresa
  const tel = [j.ddd_telefone_1, j.ddd_telefone_2].filter(Boolean).join(' / ') || null
  const email = (j.email ?? '').toLowerCase() || null
  let contato = 0
  if (tel || email) {
    const { data: existe } = await supa.from('contatos').select('id')
      .eq('cnpj_raiz', cnpj_raiz).eq('cargo', 'CADASTRO RFB').limit(1)
    if (!existe?.length) {
      const { error } = await supa.from('contatos').insert({
        cnpj_raiz, nome: 'Contato cadastral (Receita)', cargo: 'CADASTRO RFB',
        telefone: tel, email, origem: 'IMPORTACAO',
        observacao: `Dossiê gerado em ${nowIso.slice(0, 10)} · fonte pública do CNPJ`,
      })
      if (!error) contato = 1
    }
  }

  return json({
    ok: true, cnpj,
    razao_social: j.razao_social ?? null,
    decisores_criados: decisores, evidencias, contato_cadastral: contato,
    qsa_total: qsa.length,
    nota: 'Todos os registros entram para validação humana; Receita é fonte oficial (evidência CORROBORADO).',
  })
})

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } })
}
