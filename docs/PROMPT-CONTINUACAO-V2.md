# PROMPT DE CONTINUAÇÃO — Plataforma VF de Originação por Eventos (v2)

> Objetivo: codificar direto, sem re-derivar a lógica. Este documento consolida o
> que JÁ está de pé, as decisões tomadas, o domínio, e as próximas frentes com
> especificação suficiente para implementação imediata.
> **Stack decidida (não trocar): Next.js 14 + TS + Tailwind + Supabase. Deploy Vercel.**

---

## 0. PRINCÍPIO
Cérebro (dado + inteligência no Supabase) primeiro; pele (UI) depois. Não jogar fora
o que funciona. Unidade = **OPORTUNIDADE / EVENTO** — nunca "lead". Nada de recriar
o que existe; tudo aditivo.

---

## 1. O QUE JÁ ESTÁ DE PÉ (produção — Supabase `pnmasrynyiaqhxkcqygj`, repo rvgomessp-sudo/CRM)

**Banco (aplicado):**
- `empresas` (2.823; 2.330 ativas) — enriquecida (capital, situação, sócios, `tributo_principal`,
  `evento_judicial_tipo/em/peso`).
- `inscricoes` — hoje 1 principal por empresa (⚠ faltam ~110k secundárias; ver frente F5).
- `eventos` (fonte FISCAL|LICITACAO|JUDICIAL; tipo, cnpj_raiz, numero_processo, ocorrido_em, payload, hash).
  - 2.330 eventos FISCAL (backfill) + **571 JUDICIAIS reais** (185 empresas, 570 processos).
- `oportunidades` (cnpj, fonte, motor, score, estagio, **triagem** NOVO/VISTO/DESCARTADO/ABORDAR, motivo_descarte).
- `config_score` (1 linha, pesos EDITÁVEIS): viabilidade 30, porte 13, dívida 9, situação 8,
  motor 15, evento 25; faixas capital 5MM–500MM, dívida_min 3MM, ratio_ideal 1.5, ratio_max 8;
  `tributo_pesos`, `motor_pesos`, `evento_pesos`.
- `fn_score_oportunidade(valor, capital, situacao, motor, tributo, evento_peso)` → 0..100.
- `vw_fila_oportunidades` (security_invoker) — a fila ordenada; expõe score, ratio_divida_capital,
  tributo_principal, evento_judicial_tipo/em, alvo_marinheiro.
- Migrations versionadas: `sql/migrations/30_…`, `31_…`, `32_…`. 5 views antigas já `security_invoker`.

**UI (deployada em crm-x874.vercel.app):**
- Rota `/oportunidades` — fila priorizada; filtros na URL (persistem); colunas Score, Empresa, UF,
  Motor, Tributo, D/C, Evento judicial, Triagem; botões Visto/Abordar/Descartar; "Só marinheiro".
- Ficha `/empresa/[cnpj]` (Overview/Inscrições/Contatos/Interações/Proposta), Pipeline, Base PGFN, VF Solver.

**Score atual (fórmula viva):**
`oportunidade = viabilidade(dívida/capital) + porte(tributo) + dívida(log) + situação + motor + EVENTO judicial`.

---

## 2. DOMÍNIO — "MAPA DE GUERRA" (rege score e painel)

Fluxo: auto → defesa adm → recursos → constituição definitiva → inscrição dívida ativa → CDA → execução fiscal.

**Zonas de risco (eixo do score):**
- 🟢 VERDE: fase administrativa. Prevenção (A2). Baixa pressão.
- 🟡 AMARELA: inscrição em dívida ativa + CDA / protesto. Restrição, sem constrição.
- 🔴 VERMELHA: execução ajuizada. **A CITAÇÃO liga o cronômetro** (risco de bloqueio concreto).
- 🚨 SISBAJUD (1º bloqueio de conta): **o momento de sufoco** — conta trava, capital de giro some.
  MAIOR sinal de alerta. Pico de urgência.

**Regras que pesam o evento:**
- Ação ANULATÓRIA sozinha NÃO suspende exigibilidade nem para execução → "decidiu brigar, ainda vai precisar de garantia".
- MANDADO DE SEGURANÇA NÃO substitui garantia (depende de liminar; se cai, execução volta).
- EMBARGOS À EXECUÇÃO **EXIGE garantia do juízo** (Lei 6.830); jurisprudência aceita SEGURO GARANTIA → gatilho mais direto.
- Jogada sofisticada: anulatória → tutela → estrutura seguro garantia → troca dinheiro imobilizado por prêmio menor → embargos.

Regra de priorização: **o maior sinal de alerta mais recente por empresa manda.**

---

## 3. FATOS TÉCNICOS CONFIRMADOS (ao vivo)

- **DJEN / `comunicaapi.pje.jus.br/api/v1/comunicacao`**: aceita `nomeParte` e `numeroProcesso` (além de
  tribunal+data). Retorna `destinatarios[].nome`+`polo` (P=executado, A=empresa no ataque), `numero_processo`,
  `nomeClasse`, `data_disponibilizacao`, `hash`. **Sem auth.** É a ponte CNPJ→processo (busca por nome do alvo).
  Resolver validado: 279 alvos → 185 com processo (100% de resolução dos que têm).
- **Datajud (CNJ `api-publica.datajud.cnj.jus.br`)**: busca por `numeroProcesso`/classe/valor — **NÃO por parte/CNPJ**.
  Serve para, dado um processo já resolvido, ler **movimentos** (SISBAJUD, penhora, RENAJUD), valor da causa, fase.
  Padrão pronto para reaproveitar em `vf_juridico/backend/app/services/datajud.py` (query ES + mapa de 62 tribunais).
- Resolver atual: `scratchpad/resolver_judicial.py` (por nomeParte, resumível, backoff de rate limit).
- Deploy: operador não usa Git/CLI. Fluxo que funciona: PAT fine-grained (conta rvgomessp-sudo, Contents:write)
  colado no chat → commit+push na main → Vercel publica. Token de validade curta, revogar após.

---

## 4. FRENTES A CODIFICAR (ordem recomendada)

### F1 — SISBAJUD / movimentos (enriquecimento #1) — *prioridade máxima*
Objetivo: detectar o pico de urgência. Para cada `eventos.numero_processo` (JUDICIAL), consultar Datajud por
número, ler `movimentos[]`, classificar: SISBAJUD/bloqueio (art. 854), PENHORA, RENAJUD, CITAÇÃO. Gravar novos
eventos JUDICIAL (tipo=SISBAJUD/PENHORA/CITACAO) com `ocorrido_em`=data do movimento.
- Reusar o padrão de `vf_juridico` datajud.py (ES `_search` por `numeroProcesso`, mapa de tribunais).
- Onde roda: script/Edge Function. Datajud exige API key pública do CNJ (obter/config em env).
- Efeito: adicionar `SISBAJUD` (peso máx) e `PENHORA` ao `config_score.evento_pesos`; recomputar
  `empresas.evento_judicial_*` e `oportunidades.score`.

### F2 — Score por ZONA DE RISCO
Materializar `empresas.zona_risco` (VERDE/AMARELA/VERMELHA/SISBAJUD) derivada dos eventos (execução ajuizada →
VERMELHA; SISBAJUD/penhora → SISBAJUD; sem judicial + só dívida ativa → AMARELA; A2 sem ajuizamento → VERDE).
Incorporar a zona ao score (a zona pode multiplicar/limitar o fator evento). Tudo via `config_score` editável.

### F3 — PAINEL DA OPORTUNIDADE (dossiê, substitui a "ficha vitrine")
Referência visual: `comercial/Juridico - Prototipo/maquina-estado-prototype.html` (cards de caso + narrativa
de garantia). Conteúdo do painel por empresa:
- Cabeçalho: nome, CNPJ, UF, score, **zona de risco** (semáforo), evento mais grave + data.
- **Timeline processual** (os eventos JUDICIAIS ordenados: citação → penhora → SISBAJUD → embargos…).
- Leitura de risco (texto gerado da zona + regra jurídica aplicável: "embargos exige garantia" etc.).
- **Quem abordar** (decisor — ligar Econodata, F4) + sócios (já em `empresas.socios`).
- Inscrições PGFN (após F5, as N reais), dívida consolidada.
- Ação: gerar proposta (VF Solver pré-preenchido) + registrar interação.

### F4 — Econodata (decisor/contato) — *depende da doc/credencial do operador*
Botão "enriquecer contatos"; grava em `contatos` com origem=ECONODATA. NÃO inventar endpoint — usar a doc.
Cota é paga: cache + on-demand por oportunidade.

### F5 — Retífica: consolidar dívida (110k inscrições)
Reprocessar o CSV SIDA (2,4 GB, nesta máquina) aplicando a régua v2 (`sql/pgfn/20_base_final_v2.sql` no repo
gsb/local) e subir as inscrições restantes ao Supabase → o trigger `fn_recalcular_empresa` recompõe
`valor_total_devida`/`qtd_inscricoes`. Antes: corrigir a lista de prioridade de motor dentro da função (só
conhece A1/B1/B2/A2; jogar B3/B4/B5 corretamente).

### F6 — Resolver diário na nuvem
Portar `resolver_judicial.py` para Edge Function agendada (Supabase cron), rodando sobre os 2.330 (não só 279),
gravando eventos novos e atualizando o sinal judicial. Manter dedup por hash.

### F7 (arquitetura futura, NÃO agora) — Portal Holding
Multi-entidade (V&F + Vieira Mendonça) e multi-atividade (corretora/consultoria/mentoria), papéis (RLS),
motor de decisão "qual a melhor ação agora". Reconstruir a pele quando o núcleo seguro-garantia gerar negócio.

---

## 5. RESTRIÇÕES
- Migrations aditivas, idempotentes, reversíveis, versionadas em `sql/migrations/`. RLS em toda tabela nova.
- Nunca credencial no código (env). `security_invoker` nas views. TypeScript deve passar (`tsc` + `next build`).
- UI pt-BR, dark theme (globals.css), inputs com contraste. Unidade = oportunidade/evento (nunca "lead").
- Colher de `vf_juridico`: padrão Datajud + mapa de tribunais + base de conhecimento. NÃO portar a stack.
- Usar `Juridico - Prototipo/*.html` só como referência visual do painel.

---

## 6. ARRANQUE RÁPIDO (para uma nova sessão de código)
1. Ler memória do projeto (MEMORY.md) e este doc.
2. Confirmar frente (default: F1 → F2 → F3).
3. Escrever migration + código; validar `tsc`/`build`; entregar para push (fluxo PAT).
