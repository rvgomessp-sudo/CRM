# Integração VF Graph Intelligence ↔ CRM

**Decisão de arquitetura (2026-08-19):** repositórios **separados**.

| | Repositório | Stack | Deploy | Papel |
|---|---|---|---|---|
| CRM | `rvgomessp-sudo/CRM` | Next.js + Supabase | Vercel | **Verdade operacional** (funil, tarefas, validação) |
| Engine | VF Graph Intelligence v0.5.0 (repo próprio) | Python (FastAPI/CLI) | serviço/local, **nunca Vercel** | **Verdade relacional** (grafo processo→evento→hipótese→empresa→decisor) |

Por que separado: stacks diferentes (misturar Python no repo do CRM quebra o build da Vercel); e o próprio design do engine separa as verdades (grafo = Neo4j/SQLite; operacional = Supabase). A ligação é um **contrato estreito de projeção**, não um monólito.

## A ponte (migration 38, aplicada em produção)

O engine produz a projeção (`graph_to_crm_projection`) com as chaves
`crm_empresas / crm_oportunidades / crm_decisores / entity_evidence`.
O CRM a consome por `fn_ingest_crm_projection(jsonb)`.

Tabelas operacionais criadas no Supabase:

- `processos` — a espinha; o **processo é a origem**, não a empresa. Seed real: 1.539 processos de `eventos`.
- `decisores` — pessoa + classificação + poder decisório. Seed honesto: advogados dos autos como `INFLUENCIADOR_POTENCIAL`, poder `NAO_CONFIRMADO` (advogado ≠ decisor comercial).
- `hipoteses_securitarias` — saída do grafo; nasce `PENDENTE_VALIDACAO`. Vazia até o engine/validação gerar.
- `entity_evidence` — evidência graduada com datas separadas (evento/publicação/captura/confirmação).
- `graph_snapshots` — event sourcing; nunca sobrescreve.
- `vw_inscricao_processo` — a **ponte inscrição↔processo**.

## A ponte inscrição ↔ processo (a "combinação obrigatória de chaves")

A inscrição PGFN (`inscricoes.numero_inscricao`) **é a CDA** — Certidão de
Dívida Ativa, a materialização da dívida ativa. Quando **ajuizada**
(`indicador_ajuizado`), vira título de uma **Execução Fiscal**.

O vínculo com o processo usa confiança graduada, nunca forjada:

| Confiança | Condição | Volume (19/08) |
|---|---|---|
| `DETERMINISTICO_CDA_NO_TEXTO` | dígitos da CDA aparecem no teor do evento | 0 (a CDA não vem no trecho do DJEN) |
| `CORROBORADO_CNPJ_EXECFISCAL_AJUIZADA` | CNPJ + inscrição ajuizada + processo federal/estadual | 632 |
| `HIPOTESE_CNPJ` | só o CNPJ liga | 593 |

O número exato da CDA vive na **petição inicial** (DataJud), não no snippet
do DJEN — por isso o vínculo determinístico depende da ingestão DataJud.

## Fluxo do engine (subordinado ao grafo)

```
DataJud/DJEN (processo)  ->  Engine: from_process_input  ->  Grafo (snapshot)
   ->  graph_to_crm_projection  ->  fn_ingest_crm_projection  ->  Supabase
   ->  validação humana (Central)  ->  feedback volta como novo snapshot
```

Provado ponta a ponta em 2026-08-19 (com ROLLBACK, sem persistir sintético):
processo → grafo `PROCESSO_COMO_ORIGEM` classificação `PRIORIDADE` score 85 →
hipótese `SUBSTITUICAO_DE_PENHORA` → ingest `{ok:true, evidencias:6}`.

## Regras que a ponte impõe

- Sem origem processual validada, **não há oportunidade** (enriquecimento de CNPJ é `ENTITY_ENRICHMENT_ONLY`).
- Advogado/administrador **não** viram decisor comercial automaticamente.
- Hipótese securitária é hipótese; `POSSUI_GARANTIA` exige evidência confirmada/corroborada.
- "Não localizado" nunca é convertido em "inexistente".
