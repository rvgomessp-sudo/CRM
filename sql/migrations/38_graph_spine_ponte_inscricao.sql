-- ============================================================
-- 38 · ESPINHA DE GRAFO + PONTE INSCRIÇÃO↔PROCESSO
-- Integração do VF Graph Intelligence v0.5.0 ao CRM.
--
-- Mudança conceitual (prompt mestre): o PROCESSO é a origem, não a
-- empresa. Estas tabelas são a VERDADE OPERACIONAL (Supabase); a
-- verdade relacional vive no engine (grafo/Neo4j). A projeção do
-- engine (crm_empresas/oportunidades/decisores/entity_evidence)
-- entra por fn_ingest_crm_projection.
--
-- NÃO inventa dados: processos vêm de eventos reais; decisores são
-- semeados só com advogados dos autos que já temos (classificados
-- honestamente como INFLUENCIADOR_POTENCIAL, poder NAO_CONFIRMADO —
-- advogado não é decisor comercial). Hipóteses e evidências nascem
-- vazias, preenchidas pelo engine/validação humana.
-- Aditiva, idempotente. Rollback ao fim.
-- ============================================================

-- ---- Espinha processual (o processo como nó de origem) ----------
CREATE TABLE IF NOT EXISTS processos (
  numero_processo   text PRIMARY KEY,      -- 20 dígitos normalizado
  cnpj_raiz         char(8),
  tribunal          text,
  ramo              text,                  -- FEDERAL/TRABALHISTA/ESTADUAL
  classe            text,
  papel_empresa     text NOT NULL DEFAULT 'NAO_CONFIRMADO'
                    CHECK (papel_empresa IN ('DEVEDORA','CREDORA','AMBIGUO','PENDENTE','NAO_CONFIRMADO')),
  valor_processo    numeric,               -- NUNCA é a dívida PGFN
  ativo             boolean,
  primeiro_evento_em date,
  ultimo_evento_em  date,
  graph_node_id     text,
  graph_snapshot_id text,
  criado_em         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_processos_cnpj ON processos (cnpj_raiz);
ALTER TABLE processos ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS processos_all ON processos;
CREATE POLICY processos_all ON processos FOR ALL TO authenticated USING (true) WITH CHECK (true);

-- seed real: um processo por número distinto vindo de eventos JUDICIAL
INSERT INTO processos (numero_processo, cnpj_raiz, ramo, primeiro_evento_em, ultimo_evento_em, ativo)
SELECT regexp_replace(ev.numero_processo,'[^0-9]','','g') AS np,
       (array_agg(ev.cnpj_raiz ORDER BY ev.ocorrido_em DESC))[1],
       CASE substring(regexp_replace(ev.numero_processo,'[^0-9]','','g') from 14 for 1)
         WHEN '4' THEN 'FEDERAL' WHEN '5' THEN 'TRABALHISTA' WHEN '8' THEN 'ESTADUAL' ELSE 'OUTRO' END,
       min(ev.ocorrido_em), max(ev.ocorrido_em), true
FROM eventos ev
WHERE ev.fonte='JUDICIAL' AND ev.numero_processo IS NOT NULL
  AND length(regexp_replace(ev.numero_processo,'[^0-9]','','g')) = 20
GROUP BY np, CASE substring(regexp_replace(ev.numero_processo,'[^0-9]','','g') from 14 for 1)
  WHEN '4' THEN 'FEDERAL' WHEN '5' THEN 'TRABALHISTA' WHEN '8' THEN 'ESTADUAL' ELSE 'OUTRO' END
ON CONFLICT (numero_processo) DO NOTHING;

-- ---- Hipótese securitária (SAÍDA do grafo — hipótese, nunca fato) ----
CREATE TABLE IF NOT EXISTS hipoteses_securitarias (
  id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  numero_processo    text REFERENCES processos(numero_processo),
  cnpj_raiz          char(8),
  tipo               text NOT NULL,        -- GARANTIA_INICIAL/SUBSTITUICAO_DE_PENHORA/RENOVACAO/REFORCO
  score              numeric,
  status             text NOT NULL DEFAULT 'PENDENTE_VALIDACAO'
                     CHECK (status IN ('PENDENTE_VALIDACAO','VALIDADA','DESCARTADA')),
  evento_origem      text,
  justificativa      text,
  fonte              text,
  evidencia_condicao text DEFAULT 'HIPOTESE',
  graph_snapshot_id  text,
  criado_em          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_hipoteses_proc ON hipoteses_securitarias (numero_processo);
ALTER TABLE hipoteses_securitarias ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS hip_all ON hipoteses_securitarias;
CREATE POLICY hip_all ON hipoteses_securitarias FOR ALL TO authenticated USING (true) WITH CHECK (true);

-- ---- Decisores (contrato da projeção do grafo) ------------------
CREATE TABLE IF NOT EXISTS decisores (
  id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  cnpj_raiz          char(8) NOT NULL,
  nome               text NOT NULL,
  cargo              text NOT NULL DEFAULT '',
  classificacao      text NOT NULL DEFAULT 'NAO_CONFIRMADO'
                     CHECK (classificacao IN ('DECISOR_FINANCEIRO_POTENCIAL','DECISOR_JURIDICO_POTENCIAL',
                                              'PATROCINADOR_INTERNO_POTENCIAL','INFLUENCIADOR_POTENCIAL','NAO_CONFIRMADO')),
  poder_decisorio    text NOT NULL DEFAULT 'NAO_CONFIRMADO'
                     CHECK (poder_decisorio IN ('CONFIRMADO','POTENCIAL','NAO_CONFIRMADO')),
  prioridade         int DEFAULT 5,
  fonte              text,
  evidencia_condicao text DEFAULT 'HIPOTESE',
  trecho             text,
  link_fonte         text,
  graph_node_id      text,
  graph_snapshot_id  text,
  validado_por       uuid REFERENCES profiles(id),
  validado_em        timestamptz,
  criado_em          timestamptz NOT NULL DEFAULT now(),
  UNIQUE (cnpj_raiz, nome, cargo)
);
CREATE INDEX IF NOT EXISTS idx_decisores_cnpj ON decisores (cnpj_raiz);
ALTER TABLE decisores ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS decisores_all ON decisores;
CREATE POLICY decisores_all ON decisores FOR ALL TO authenticated USING (true) WITH CHECK (true);

-- seed HONESTO: advogados dos autos já coletados → INFLUENCIADOR_POTENCIAL,
-- poder NAO_CONFIRMADO (advogado dos autos NÃO é decisor comercial; a ordem exige)
INSERT INTO decisores (cnpj_raiz, nome, cargo, classificacao, poder_decisorio, prioridade,
                       fonte, evidencia_condicao, graph_node_id)
SELECT DISTINCT ON (e.cnpj_raiz, e.advogados->0->>'nome')
  e.cnpj_raiz,
  e.advogados->0->>'nome',
  'Advogado dos autos' || CASE WHEN e.advogados->0->>'oab' IS NOT NULL
    THEN ' · OAB ' || (e.advogados->0->>'oab') || coalesce('/'||(e.advogados->0->>'uf'),'') ELSE '' END,
  'INFLUENCIADOR_POTENCIAL', 'NAO_CONFIRMADO', 4,
  'DJEN_COMUNICA', 'CORROBORADO', 'adv:'||(e.advogados->0->>'oab')
FROM eventos e
WHERE e.fonte='JUDICIAL' AND e.advogados IS NOT NULL
  AND (e.advogados->0->>'nome') IS NOT NULL
  AND upper(e.advogados->0->>'nome') NOT LIKE '%REVEL%'
ON CONFLICT (cnpj_raiz, nome, cargo) DO NOTHING;

-- ---- Evidência graduada (contrato do grafo, datas separadas) ----
CREATE TABLE IF NOT EXISTS entity_evidence (
  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type       text NOT NULL,        -- COMPANY/PERSON/PROCESS/RELATIONSHIP/EMAIL/PHONE
  entity_ref        text NOT NULL,
  source            text,
  link_fonte        text,
  trecho            text,
  confidence        numeric,
  condicao          text DEFAULT 'HIPOTESE'
                    CHECK (condicao IN ('CONFIRMADO','CORROBORADO','HIPOTESE','LACUNA','CONFLITO','DESATUALIZADO','NAO_CONFIRMAVEL')),
  data_evento       date,
  data_publicacao   date,
  data_captura      timestamptz DEFAULT now(),
  data_confirmacao  timestamptz,
  graph_snapshot_id text,
  criado_em         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_evidence_entity ON entity_evidence (entity_type, entity_ref);
ALTER TABLE entity_evidence ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS evidence_all ON entity_evidence;
CREATE POLICY evidence_all ON entity_evidence FOR ALL TO authenticated USING (true) WITH CHECK (true);

-- ---- Snapshots do grafo (event sourcing; nunca sobrescreve) ----
CREATE TABLE IF NOT EXISTS graph_snapshots (
  graph_id                    text PRIMARY KEY,
  parent_graph_id             text,
  origin_status               text,
  opportunity_score           numeric,
  opportunity_classification  text,
  origem                      text,
  payload                     jsonb,
  criado_em                   timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE graph_snapshots ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS snap_all ON graph_snapshots;
CREATE POLICY snap_all ON graph_snapshots FOR ALL TO authenticated USING (true) WITH CHECK (true);

-- ============================================================
-- PONTE INSCRIÇÃO ↔ PROCESSO (a "combinação obrigatória de chaves")
-- Liga a CDA (inscricoes.numero_inscricao, ajuizada) ao processo de
-- execução fiscal da MESMA empresa. Confiança:
--   DETERMINISTICO — dígitos da CDA aparecem no texto do evento
--   CORROBORADO    — CNPJ + inscrição ajuizada + processo federal/estadual
-- Nunca afirma o vínculo sem uma dessas condições.
-- ============================================================
CREATE OR REPLACE VIEW vw_inscricao_processo
WITH (security_invoker = on) AS
WITH exec AS (
  SELECT DISTINCT p.numero_processo, p.cnpj_raiz, p.ramo,
         string_agg(DISTINCT coalesce(e.texto,''), ' ') AS textos
  FROM processos p
  LEFT JOIN eventos e ON e.fonte='JUDICIAL'
        AND regexp_replace(e.numero_processo,'[^0-9]','','g') = p.numero_processo
  WHERE p.ramo IN ('FEDERAL','ESTADUAL')
  GROUP BY p.numero_processo, p.cnpj_raiz, p.ramo
)
SELECT
  i.cnpj_raiz,
  i.numero_inscricao          AS cda,
  i.valor_numerico            AS divida_cda,
  i.indicador_ajuizado,
  x.numero_processo,
  x.ramo,
  CASE
    WHEN i.numero_inscricao IS NOT NULL
     AND x.textos ILIKE '%'||regexp_replace(i.numero_inscricao,'[^0-9]','','g')||'%'
     AND length(regexp_replace(i.numero_inscricao,'[^0-9]','','g')) >= 8
      THEN 'DETERMINISTICO_CDA_NO_TEXTO'
    WHEN i.indicador_ajuizado THEN 'CORROBORADO_CNPJ_EXECFISCAL_AJUIZADA'
    ELSE 'HIPOTESE_CNPJ'
  END AS confianca_vinculo
FROM inscricoes i
JOIN exec x ON x.cnpj_raiz = i.cnpj_raiz;

-- ============================================================
-- SINCRONIZADOR: o engine chama isto com a projeção (JSON do grafo)
-- Idempotente por graph_node_id/snapshot. NÃO cria oportunidade sem
-- origem processual validada (regra do prompt mestre).
-- ============================================================
CREATE OR REPLACE FUNCTION fn_ingest_crm_projection(p_projection jsonb)
RETURNS jsonb LANGUAGE plpgsql SECURITY INVOKER SET search_path = public AS $$
DECLARE
  v_snap text := p_projection->>'graph_snapshot_id';
  v_dec int := 0; v_ev int := 0;
  r jsonb;
BEGIN
  IF (p_projection->>'source_of_truth') <> 'GRAPH' THEN
    RAISE EXCEPTION 'projeção inválida: source_of_truth deve ser GRAPH';
  END IF;

  -- snapshot (event sourcing)
  INSERT INTO graph_snapshots (graph_id, origem, payload)
  VALUES (coalesce(v_snap, gen_random_uuid()::text), 'PROJECTION_INGEST', p_projection)
  ON CONFLICT (graph_id) DO NOTHING;

  -- decisores
  FOR r IN SELECT * FROM jsonb_array_elements(coalesce(p_projection->'crm_decisores','[]'::jsonb)) LOOP
    INSERT INTO decisores (cnpj_raiz, nome, cargo, classificacao, poder_decisorio, prioridade,
                           fonte, graph_node_id, graph_snapshot_id)
    VALUES (
      substr(regexp_replace(coalesce(r->>'cnpj',''),'[^0-9]','','g'),1,8),
      r->>'nome', coalesce(r->>'cargo',''),
      coalesce(r->>'classificacao','NAO_CONFIRMADO'),
      coalesce(r->>'poder_decisorio','NAO_CONFIRMADO'),
      coalesce((r->>'prioridade')::int, 5),
      'GRAPH_PROJECTION', r->>'pessoa_graph_node_id', v_snap)
    ON CONFLICT (cnpj_raiz, nome, cargo) DO UPDATE
      SET classificacao = EXCLUDED.classificacao,
          poder_decisorio = EXCLUDED.poder_decisorio,
          graph_snapshot_id = EXCLUDED.graph_snapshot_id;
    v_dec := v_dec + 1;
  END LOOP;

  -- evidências
  FOR r IN SELECT * FROM jsonb_array_elements(coalesce(p_projection->'entity_evidence','[]'::jsonb)) LOOP
    INSERT INTO entity_evidence (entity_type, entity_ref, source, confidence, condicao, trecho, graph_snapshot_id)
    VALUES (coalesce(r->>'entity_type','RELATIONSHIP'), r->>'entity_id', r->>'source',
            (r->>'confidence')::numeric,
            CASE lower(coalesce(r->>'status',''))
              WHEN 'confirmed' THEN 'CONFIRMADO' WHEN 'corroborated' THEN 'CORROBORADO'
              WHEN 'hypothesis' THEN 'HIPOTESE'  WHEN 'gap' THEN 'LACUNA'
              WHEN 'conflict' THEN 'CONFLITO'    WHEN 'outdated' THEN 'DESATUALIZADO'
              WHEN 'unconfirmable' THEN 'NAO_CONFIRMAVEL'
              ELSE upper(coalesce(r->>'status','HIPOTESE')) END,
            r->>'excerpt', v_snap);
    v_ev := v_ev + 1;
  END LOOP;

  RETURN jsonb_build_object('ok', true, 'snapshot', v_snap, 'decisores', v_dec, 'evidencias', v_ev);
END $$;

-- ROLLBACK:
-- DROP FUNCTION IF EXISTS fn_ingest_crm_projection(jsonb);
-- DROP VIEW IF EXISTS vw_inscricao_processo;
-- DROP TABLE IF EXISTS graph_snapshots, entity_evidence, decisores, hipoteses_securitarias, processos CASCADE;
