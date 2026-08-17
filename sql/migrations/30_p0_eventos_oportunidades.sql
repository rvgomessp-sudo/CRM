-- ============================================================
-- MIGRATION 30 — P0: Fundação orientada a eventos
-- Vazquez & Fonseca | CRM V3 (Supabase pnmasrynyiaqhxkcqygj)
--
-- Aditiva, idempotente e reversível. NÃO altera dado existente.
-- Introduz:
--   * eventos          — fato capturado de uma fonte (fiscal/licitação/judicial)
--   * oportunidades     — necessidade de garantia derivada de evento(s)
--   * config_score      — faixas e pesos EDITÁVEIS (nada cravado no código)
--   * vw_fila_oportunidades — a fila priorizada
--   * conserto das 5 views SECURITY DEFINER (furavam o RLS)
--   * backfill: 1 evento fiscal + 1 oportunidade por empresa ativa
--
-- Rollback ao final do arquivo (comentado).
-- ============================================================

-- ---- 1. TIPOS -------------------------------------------------
DO $$ BEGIN
  CREATE TYPE fonte_evento AS ENUM ('FISCAL', 'LICITACAO', 'JUDICIAL');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE triagem_status AS ENUM ('NOVO', 'VISTO', 'DESCARTADO', 'ABORDAR');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ---- 2. EVENTOS ----------------------------------------------
-- Cidadão de primeira classe. Toda fonte grava aqui, no mesmo formato.
CREATE TABLE IF NOT EXISTS eventos (
  id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  fonte              fonte_evento NOT NULL,
  tipo               TEXT NOT NULL,             -- ex: DIVIDA_ATIVA, CITACAO, SISBAJUD, PENHORA, HOMOLOGACAO
  cnpj_raiz          CHAR(8),                   -- pode ser nulo até resolver processo→CNPJ (judicial)
  numero_processo    TEXT,                      -- chave estável da fonte judicial
  ocorrido_em        DATE,                      -- data do fato (inscrição, publicação DJE, homologação)
  capturado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
  payload            JSONB NOT NULL DEFAULT '{}'::jsonb,
  hash               TEXT NOT NULL,             -- dedup: mesmo fato não entra duas vezes
  gerou_oportunidade BOOLEAN NOT NULL DEFAULT FALSE,
  criado_em          TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uq_eventos_hash UNIQUE (hash)
);

CREATE INDEX IF NOT EXISTS ix_eventos_cnpj      ON eventos (cnpj_raiz);
CREATE INDEX IF NOT EXISTS ix_eventos_fonte     ON eventos (fonte);
CREATE INDEX IF NOT EXISTS ix_eventos_processo  ON eventos (numero_processo);
CREATE INDEX IF NOT EXISTS ix_eventos_ocorrido  ON eventos (ocorrido_em);

-- ---- 3. OPORTUNIDADES ----------------------------------------
-- Estado comercial que PERSISTE. A triagem é o "quem eu já vi ontem".
CREATE TABLE IF NOT EXISTS oportunidades (
  id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  cnpj_raiz          CHAR(8) NOT NULL REFERENCES empresas(cnpj_raiz) ON DELETE CASCADE,
  fonte              fonte_evento NOT NULL DEFAULT 'FISCAL',
  evento_origem_id   UUID REFERENCES eventos(id) ON DELETE SET NULL,
  motor              motor_tipo,
  score              NUMERIC(6,2),              -- materializado p/ ordenar rápido; a view recalcula
  estagio            pipeline_stage NOT NULL DEFAULT 'base_pgfn',
  triagem            triagem_status NOT NULL DEFAULT 'NOVO',
  triado_por         UUID REFERENCES profiles(id) ON DELETE SET NULL,
  triado_em          TIMESTAMPTZ,
  motivo_descarte    TEXT,
  criado_em          TIMESTAMPTZ NOT NULL DEFAULT now(),
  atualizado_em      TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uq_oportunidade_empresa_fonte UNIQUE (cnpj_raiz, fonte)
);

CREATE INDEX IF NOT EXISTS ix_oport_triagem ON oportunidades (triagem);
CREATE INDEX IF NOT EXISTS ix_oport_score   ON oportunidades (score DESC);
CREATE INDEX IF NOT EXISTS ix_oport_fonte   ON oportunidades (fonte);

-- ---- 4. CONFIG DE SCORE (editável, 1 linha) ------------------
CREATE TABLE IF NOT EXISTS config_score (
  id            INT PRIMARY KEY DEFAULT 1,
  capital_min   NUMERIC NOT NULL DEFAULT 5000000,     -- piso "não é micro"
  capital_max   NUMERIC NOT NULL DEFAULT 500000000,   -- teto "não é gigante recorrente"
  divida_min    NUMERIC NOT NULL DEFAULT 3000000,     -- piso de honorário relevante
  peso_divida   INT NOT NULL DEFAULT 30,
  peso_capital  INT NOT NULL DEFAULT 25,
  peso_situacao INT NOT NULL DEFAULT 15,
  motor_pesos   JSONB NOT NULL DEFAULT
    '{"A1":30,"B1":24,"B2":20,"A2":16,"B3":14,"B4":12,"B5":10}'::jsonb,
  atualizado_em TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT ck_config_singleton CHECK (id = 1)
);

INSERT INTO config_score (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

-- ---- 5. RLS nas tabelas novas --------------------------------
ALTER TABLE eventos       ENABLE ROW LEVEL SECURITY;
ALTER TABLE oportunidades ENABLE ROW LEVEL SECURITY;
ALTER TABLE config_score  ENABLE ROW LEVEL SECURITY;

DO $$ BEGIN
  CREATE POLICY pol_eventos_all ON eventos
    FOR ALL TO authenticated USING (true) WITH CHECK (true);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE POLICY pol_oportunidades_all ON oportunidades
    FOR ALL TO authenticated USING (true) WITH CHECK (true);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE POLICY pol_config_score_all ON config_score
    FOR ALL TO authenticated USING (true) WITH CHECK (true);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ---- 6. FUNÇÃO DE SCORE (0–100, lê config_score) -------------
-- search_path fixado (corrige o warning de search_path mutável).
CREATE OR REPLACE FUNCTION fn_score_oportunidade(
  p_valor_total   NUMERIC,
  p_capital       NUMERIC,
  p_situacao      TEXT,
  p_motor         motor_tipo
) RETURNS NUMERIC
LANGUAGE sql STABLE
SET search_path = public
AS $$
  SELECT GREATEST(0, ROUND((
      -- dívida acima do piso, com saturação logarítmica
      CASE WHEN p_valor_total >= c.divida_min
           THEN LEAST(c.peso_divida::numeric,
                      (c.peso_divida * ln(p_valor_total / c.divida_min + 1) / ln(50))::numeric)
           ELSE 0 END
      -- capital na faixa marinheiro (nulo = meio-termo, não zera)
    + CASE WHEN p_capital BETWEEN c.capital_min AND c.capital_max THEN c.peso_capital::numeric
           WHEN p_capital IS NULL THEN (c.peso_capital * 0.3)::numeric
           ELSE 0 END
      -- situação cadastral ativa
    + CASE WHEN p_situacao = 'ATIVA' THEN c.peso_situacao::numeric ELSE 0 END
      -- motor (peso configurável por tipo)
    + COALESCE((c.motor_pesos ->> p_motor::text)::numeric, 0)
  )::numeric, 2))
  FROM config_score c WHERE c.id = 1;
$$;

-- ---- 7. FILA PRIORIZADA (view, security_invoker) -------------
DROP VIEW IF EXISTS vw_fila_oportunidades;
CREATE VIEW vw_fila_oportunidades
WITH (security_invoker = on) AS
SELECT
  o.id                AS oportunidade_id,
  o.cnpj_raiz,
  e.cnpj_completo,
  e.nome_devedor,
  e.uf_devedor,
  o.fonte,
  o.motor,
  o.estagio,
  o.triagem,
  o.triado_em,
  o.motivo_descarte,
  e.qtd_inscricoes,
  e.valor_total_devida,
  e.valor_maior_inscricao,
  e.capital_social,
  e.cnpj_situacao,
  e.seguradora_alvo,
  e.prioridade,
  fn_score_oportunidade(e.valor_total_devida, e.capital_social, e.cnpj_situacao, o.motor) AS score,
  -- flag de alvo "marinheiro" pela faixa configurada (não filtra, só marca)
  (e.cnpj_situacao = 'ATIVA'
    AND e.valor_total_devida >= (SELECT divida_min  FROM config_score WHERE id=1)
    AND e.capital_social BETWEEN (SELECT capital_min FROM config_score WHERE id=1)
                             AND (SELECT capital_max FROM config_score WHERE id=1)) AS alvo_marinheiro
FROM oportunidades o
JOIN empresas e ON e.cnpj_raiz = o.cnpj_raiz
WHERE e.ativo AND NOT e.excluido
-- descartados NÃO são excluídos aqui: a tela filtra por triagem,
-- para permitir "ver descartados" e desfazer um descarte.
ORDER BY score DESC NULLS LAST, e.valor_total_devida DESC;

-- ---- 8. CONSERTO das 5 views SECURITY DEFINER ----------------
-- Passam a respeitar o RLS de quem consulta (security_invoker).
ALTER VIEW vw_dashboard_kpis  SET (security_invoker = on);
ALTER VIEW vw_funil_estagio   SET (security_invoker = on);
ALTER VIEW vw_sla_vencido     SET (security_invoker = on);
ALTER VIEW vw_fazer_amanha    SET (security_invoker = on);
ALTER VIEW vw_relatorio_hoje  SET (security_invoker = on);

-- ---- 9. BACKFILL retroativo ----------------------------------
-- 9a. 1 evento FISCAL por empresa ativa (a partir da inscrição já carregada)
INSERT INTO eventos (fonte, tipo, cnpj_raiz, ocorrido_em, payload, hash, gerou_oportunidade)
SELECT
  'FISCAL', 'DIVIDA_ATIVA', e.cnpj_raiz,
  i.data_inscricao,
  jsonb_build_object(
    'numero_inscricao', i.numero_inscricao,
    'situacao', i.situacao_inscricao,
    'valor_inscricao', i.valor_numerico,
    'origem', 'backfill_migration_30'),
  'FISCAL:' || e.cnpj_raiz || ':backfill',
  TRUE
FROM empresas e
LEFT JOIN LATERAL (
  SELECT numero_inscricao, situacao_inscricao, valor_numerico, data_inscricao
  FROM inscricoes WHERE cnpj_raiz = e.cnpj_raiz
  ORDER BY valor_numerico DESC NULLS LAST LIMIT 1
) i ON TRUE
WHERE e.ativo AND NOT e.excluido
ON CONFLICT (hash) DO NOTHING;

-- 9b. 1 oportunidade FISCAL por empresa ativa, ligada ao evento
INSERT INTO oportunidades (cnpj_raiz, fonte, evento_origem_id, motor, estagio, score, triagem)
SELECT
  e.cnpj_raiz, 'FISCAL', ev.id, e.motor, e.estagio,
  fn_score_oportunidade(e.valor_total_devida, e.capital_social, e.cnpj_situacao, e.motor),
  'NOVO'
FROM empresas e
LEFT JOIN eventos ev
  ON ev.cnpj_raiz = e.cnpj_raiz AND ev.hash = 'FISCAL:' || e.cnpj_raiz || ':backfill'
WHERE e.ativo AND NOT e.excluido
ON CONFLICT (cnpj_raiz, fonte) DO NOTHING;

-- ============================================================
-- ROLLBACK (rodar manualmente se precisar desfazer):
--   DROP VIEW IF EXISTS vw_fila_oportunidades;
--   DROP FUNCTION IF EXISTS fn_score_oportunidade(numeric,numeric,text,motor_tipo);
--   DROP TABLE IF EXISTS oportunidades;
--   DROP TABLE IF EXISTS eventos;
--   DROP TABLE IF EXISTS config_score;
--   DROP TYPE IF EXISTS triagem_status;
--   DROP TYPE IF EXISTS fonte_evento;
--   -- (as 5 views voltam a definer com: ALTER VIEW ... SET (security_invoker = off);)
-- Nada em empresas/inscricoes/interacoes/contatos/propostas é tocado.
-- ============================================================
