# AUDITORIA — CHECKPOINT 2026-08-19

Branch de trabalho: `codex/reconstrucao-crm-inteligente` (criada a partir de `main@85826a2`).
Regra vigente: **nenhum deploy, push ou alteração de produção sem autorização expressa.**

## 1. Estado do repositório (verificado)

- Remoto: `github.com/rvgomessp-sudo/CRM.git` · branch principal `main`, sincronizada com `origin/main` em `85826a2`.
- Árvore limpa no momento do checkpoint; sem arquivos não rastreados relevantes (`.env.local` ignorado por design).
- Histórico recente: `85826a2` (sql versionado), `daa90dd` (pauta+KPIs), `cb0f4fa` (DJEN na ficha).
- Rota `/importar` citada no README **não existe** (removida; docs/CHECKPOINT_SESSAO.md a marca obsoleta).
- `next.config.js`: `typescript.ignoreBuildErrors` e `eslint.ignoreDuringBuilds` **ativos** (verificação desligada).
- Dependências instaladas e **não usadas**: `papaparse`, `recharts`, `date-fns`, `date-fns-tz`.

## 2. Ambiente e runtime

- `.env.local`: só `NEXT_PUBLIC_*` (URL do projeto + anon key). **Não há service-role em lugar nenhum do projeto** — o app inteiro opera com anon key + sessão do usuário.
- Anon key local estava PLACEHOLDER; corrigida em 19/08 via API de chaves publicáveis (arquivo gitignored; nada registrado no repo).
- Build local: **passa** (11 rotas; ver §6).
- Runtime local: servidor Next sobe; superfícies autenticadas exigem login de usuário real — **operador humano precisa validar o fluxo logado** (o agente não digita senhas, por política).

## 3. Fluxo real (fonte → tela), como está hoje

```
PGFN (CSV LAI, 21,5M/40M inscrições)         [MANUAL, Postgres local]
  └─ régua marinheiro (sql/pgfn/12) ────────► carga única no Supabase (07-08/08)
DJEN Comunica (por processo)                  [MANUAL, scripts fora do repo]
  └─ enriquecimento teor+advogados ─────────► eventos.texto/advogados (18/08)
Receita (minhareceita.org)                    [MANUAL, script avulso]
  └─ telefone cadastral ────────────────────► contatos (19/08, 19 registros)
Supabase (RLS authenticated USING(true))
  └─ views vw_fila/kpis/funil/sla ──────────► Next.js (browser → Supabase direto)
      └─ 7 telas ─────────────────────────── operador (Rodrigo/Ana)
```
**Não existe** no produto: coleta agendada, fila, job, webhook, e-mail, API própria (`app/api` ausente), realtime, storage.

## 4. Classificação dos componentes

| Componente | Estado | Evidência/observação |
|---|---|---|
| Autenticação (login/middleware/guard/logout) | IMPLEMENTADO | Sem testes; sem MFA/reset; middleware engole erro (`catch {}`) e o guard do layout é a rede real |
| RLS | PARCIAL | Tudo `TO authenticated USING(true)`; 3 views `SECURITY DEFINER` = ERROR no linter Supabase |
| RBAC/perfis | PROTÓTIPO | `profiles.papel` só decorativo (sidebar); não gate nada |
| Multi-tenant (modelo alunos) | BLOQUEADO | RLS atual entrega a base inteira a qualquer login |
| Telas: dashboard, fila, pipeline, base-pgfn, empresa, pauta | IMPLEMENTADO | `base-pgfn` redundante c/ fila; `empresa` monólito 975 linhas |
| VF Solver | PARCIAL | v1; spec v2 (`docs/SPEC_VF_SOLVER_V2.md`) = ESPECIFICADO; sem PDF; cenários únicos |
| Score PORTE×VIABILIDADE×EVENTO (fn v3 + config_score) | IMPLEMENTADO | Pesos em dado; **calibração/precisão-recall: NÃO LOCALIZADO** |
| Zona de risco (34) | IMPLEMENTADO c/ defeito | CASE nunca produz VERDE; legenda VERDE existe na UI |
| Views vw_fila/kpis/funil/sla | IMPLEMENTADO | `vw_fazer_amanha`/`vw_relatorio_hoje` existem e **nenhuma tela consome** |
| Carga PGFN | PARCIAL | Única (07-08/08); **1 inscrição/empresa** (trigger sobrescreve — defeito conhecido); ~110k secundárias só no Postgres local |
| Coleta DJEN | PROTÓTIPO | Scripts fora do repo; execução manual 18/08; 1.680 eventos enriquecidos; 27 processos sem retorno; 32 sem advogado (réu revel) |
| Coleta recorrente / cron / fila / jobs | NÃO LOCALIZADO | Zero `fetch` externo no app; sem Edge Function; sem vercel.json |
| API CNPJ (contatos RFB) | PROTÓTIPO | Script avulso 19/08; 19/20 telefones; e-mails não expostos pela fonte |
| E-mail (envio/leitura) | NÃO LOCALIZADO | Decisão de negócio: será Outlook |
| OSINT | PROTÓTIPO (externo) | `C:\pgfn_4\VF_OSINT_v0.3.0` — 4.550 linhas, 434 de testes, evidência graduada; **desconectado do CRM** |
| SISBAJUD (acesso direto) | NÃO LOCALIZADO | Apenas menções em publicações DJEN; **nunca alegar acesso** |
| Playbook P0–P5 | IMPLEMENTADO (SQL versionado) | `sql/pauta_selecao_playbook.sql`; execução manual; sem estados de validação (→ migration 36) |
| Pauta (tabela + tela) | IMPLEMENTADO | Migration 35; 20 registros de 20/08 carregados em produção |
| Estados operacionais + evidência (Fase 2/4 da ordem) | ESPECIFICADO → nesta branch | Migration 36 + fixture, **não aplicadas** (aguardam autorização) |
| Trilha de auditoria | PARCIAL | `historico_estagios` só p/ estágio; sem trilha geral de decisões |
| Testes / CI (CRM) | NÃO LOCALIZADO | Nenhum diretório de teste; nenhum workflow; scripts de teste desta branch em `tests/` |
| Relatórios / export PDF/XLSX | NÃO LOCALIZADO | `propostas.pdf_url` existe e nada preenche |
| Deploy (crm-x874.vercel.app) | VALIDADO | Prova: hash do runtime webpack (`webpack-91b2d14d…`) idêntico entre build local com `/pauta` e produção; commits `daa90dd`/`85826a2` |
| Observabilidade / feature flags / busca | NÃO LOCALIZADO | — |

## 5. Banco (produção, verificado em 19/08)

11 tabelas. Volumes: empresas 2.823 · inscricoes 2.823 (1/empresa — defeito) · eventos 4.037 (1.680 enriquecidos; 622 de ago/26; 1.222 pré-2025) · oportunidades 2.330 (pós-19/08: 2.298 NOVO, 23 ABORDAR, 6 VISTO, 3 DESCARTADO) · pauta 20 · contatos 23 · interacoes 13 · propostas 0 · consultas_seguradora 0.
Advisors de segurança: 3 ERROR (`SECURITY DEFINER` em vw_funil_estagio, vw_dashboard_kpis, vw_sla_vencido) + WARNs (search_path mutável ×4, `fn_handle_new_user` executável por anon, leaked-password protection off, pg_trgm em public).

## 6. Divergências produção ↔ repositório

Sanadas em 19/08: migration 35 e playbook versionados. **Permanecem sem migration versionada**: `contatos` (tabela inteira), `empresas.desfecho/tipo_fechamento/motivo_encerramento/motivo_obs/fechado_em`, `empresas.socios`, `eventos.texto/advogados/numero_comunicacao/link_publicacao/enriquecido_em`, definições atuais de `vw_dashboard_kpis`/`vw_funil_estagio` (campos além do schema base).

## 7. Materiais da ordem — status

- Repositório: OK (este clone).
- PDF "Pauta de Prospecção 20_08": conteúdo íntegro reproduzido pela tabela `pauta` + artifact HTML de origem; os "20 candidatos" e seus 4 avisos são auditáveis via fixture (Fase 2).
- Imagens de referência: **6 de 9 recebidas** (Monitoramento, Proposta, Alertas, Dossiê+Solver, Pessoas & Escritórios, Execuções). 3 restantes + 3 capturas do CRM atual: PENDENTES (não bloqueiam).

## 8. Riscos imediatos

1. Única base é a produção — não há ambiente de teste; qualquer migration é mudança em produção (autorização por lote).
2. Verificação de build desligada mascara erros de tipo.
3. Dívida consolidada errada na tela (1 inscrição/empresa) até a Fase 0 do plano de dados.
4. Score legado da pauta exposto sem fórmula/versão — a ordem exige explicabilidade (Fase 8).
