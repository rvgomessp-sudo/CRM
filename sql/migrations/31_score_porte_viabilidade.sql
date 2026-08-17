-- ============================================================
-- MIGRATION 31 — Score de oportunidade REAL
-- Vazquez & Fonseca | CRM V3 (Supabase pnmasrynyiaqhxkcqygj)
--
-- Reescreve o score para medir OPORTUNIDADE, não tamanho de dívida:
--   Oportunidade = PORTE × VIABILIDADE × EVENTO × honorário
--   - PORTE      : tipo de tributo (IPI/importação = indústria de porte)
--   - VIABILIDADE: razão dívida/capital ("quer ser o fiador dele?")
--   - EVENTO     : motor (proxy até o motor judicial entrar)
--   - honorário  : magnitude da dívida (saturada), agora ponderada
--
-- Aditiva e reversível. Só toca em config_score, na função de score,
-- na view da fila e recomputa oportunidades.score. Não altera a base.
-- ============================================================

-- ---- 1. empresas.tributo_principal (categoria de porte) ------
ALTER TABLE empresas ADD COLUMN IF NOT EXISTS tributo_principal TEXT;

-- classificador reutilizável (mesma regra do backfill)
-- backfill a partir da inscrição de MAIOR valor de cada empresa
WITH cls AS (
  SELECT DISTINCT ON (i.cnpj_raiz)
    i.cnpj_raiz,
    CASE
      WHEN i.receita_principal ILIKE '%IPI%'                                    THEN 'IPI'
      WHEN i.receita_principal ILIKE '%IMPORTA%'                                THEN 'IMPORTACAO'
      WHEN i.receita_principal ILIKE '%IRRF%'                                   THEN 'IRRF'
      WHEN i.receita_principal ILIKE '%COFINS%'                                 THEN 'COFINS'
      WHEN i.receita_principal ILIKE '%PIS%'                                    THEN 'PIS'
      WHEN i.receita_principal ILIKE '%IRPJ%'                                   THEN 'IRPJ'
      WHEN i.receita_principal ILIKE '%CSLL%'                                   THEN 'CSLL'
      WHEN i.receita_principal ILIKE '%PREVID%' OR i.receita_principal ILIKE '%INSS%' THEN 'PREVIDENCIARIA'
      WHEN i.receita_principal ILIKE '%MULTA%'                                  THEN 'MULTA'
      ELSE 'OUTROS'
    END AS categoria
  FROM inscricoes i
  ORDER BY i.cnpj_raiz, i.valor_numerico DESC NULLS LAST
)
UPDATE empresas e SET tributo_principal = cls.categoria
FROM cls WHERE cls.cnpj_raiz = e.cnpj_raiz;

-- ---- 2. config_score: novos parâmetros (viabilidade + porte) --
ALTER TABLE config_score ADD COLUMN IF NOT EXISTS peso_viabilidade INT     NOT NULL DEFAULT 35;
ALTER TABLE config_score ADD COLUMN IF NOT EXISTS peso_porte        INT     NOT NULL DEFAULT 15;
ALTER TABLE config_score ADD COLUMN IF NOT EXISTS ratio_ideal       NUMERIC NOT NULL DEFAULT 1.5;  -- dívida/capital <= ideal = viável
ALTER TABLE config_score ADD COLUMN IF NOT EXISTS ratio_max         NUMERIC NOT NULL DEFAULT 8;    -- dívida/capital >= max = não-segurável
ALTER TABLE config_score ADD COLUMN IF NOT EXISTS tributo_pesos     JSONB   NOT NULL DEFAULT
  '{"IPI":15,"IMPORTACAO":15,"IRRF":11,"COFINS":11,"PIS":9,"IRPJ":8,"CSLL":8,"PREVIDENCIARIA":5,"MULTA":9,"OUTROS":6}'::jsonb;

-- rebalanceia os pesos existentes p/ o total máximo ficar ~100
UPDATE config_score SET
  peso_divida  = 15,
  peso_situacao = 10,
  motor_pesos  = '{"A1":25,"B1":20,"B2":17,"A2":13,"B3":12,"B4":10,"B5":8}'::jsonb,
  atualizado_em = now()
WHERE id = 1;
-- max: viabilidade 35 + porte 15 + dívida 15 + situação 10 + motor 25 = 100

-- ---- 3. função de score (nova assinatura, com tributo) --------
DROP VIEW IF EXISTS vw_fila_oportunidades;
DROP FUNCTION IF EXISTS fn_score_oportunidade(numeric, numeric, text, motor_tipo);

CREATE FUNCTION fn_score_oportunidade(
  p_valor_total  NUMERIC,
  p_capital      NUMERIC,
  p_situacao     TEXT,
  p_motor        motor_tipo,
  p_tributo      TEXT
) RETURNS NUMERIC
LANGUAGE sql STABLE
SET search_path = public
AS $$
  SELECT GREATEST(0, ROUND((
      -- honorário: magnitude da dívida acima do piso (saturação log)
      CASE WHEN p_valor_total >= c.divida_min
           THEN LEAST(c.peso_divida::numeric,
                      (c.peso_divida * ln(p_valor_total / c.divida_min + 1) / ln(50))::numeric)
           ELSE 0 END
      -- VIABILIDADE: dívida/capital. <= ratio_ideal = cheio; >= ratio_max = zero.
    + CASE
        WHEN p_capital IS NULL OR p_capital <= 0 THEN (c.peso_viabilidade * 0.4)::numeric
        ELSE c.peso_viabilidade * GREATEST(0::numeric, LEAST(1::numeric,
               ((c.ratio_max - (p_valor_total / p_capital)) /
                NULLIF(c.ratio_max - c.ratio_ideal, 0))::numeric))
      END
      -- PORTE: tipo de tributo
    + COALESCE((c.tributo_pesos ->> COALESCE(p_tributo,'OUTROS'))::numeric,
               (c.tributo_pesos ->> 'OUTROS')::numeric)
      -- situação cadastral ativa
    + CASE WHEN p_situacao = 'ATIVA' THEN c.peso_situacao::numeric ELSE 0 END
      -- EVENTO (proxy: motor; será reforçado pelo motor judicial)
    + COALESCE((c.motor_pesos ->> p_motor::text)::numeric, 0)
  )::numeric, 2))
  FROM config_score c WHERE c.id = 1;
$$;

-- ---- 4. view da fila (expõe ratio e tributo p/ a tela explicar) --
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
  e.tributo_principal,
  round(e.valor_total_devida / NULLIF(e.capital_social,0), 1) AS ratio_divida_capital,
  e.seguradora_alvo,
  e.prioridade,
  fn_score_oportunidade(e.valor_total_devida, e.capital_social, e.cnpj_situacao, o.motor, e.tributo_principal) AS score,
  (e.cnpj_situacao = 'ATIVA'
    AND e.valor_total_devida >= (SELECT divida_min  FROM config_score WHERE id=1)
    AND e.capital_social BETWEEN (SELECT capital_min FROM config_score WHERE id=1)
                             AND (SELECT capital_max FROM config_score WHERE id=1)) AS alvo_marinheiro
FROM oportunidades o
JOIN empresas e ON e.cnpj_raiz = o.cnpj_raiz
WHERE e.ativo AND NOT e.excluido
ORDER BY score DESC NULLS LAST, e.valor_total_devida DESC;

-- ---- 5. recomputa o score materializado das oportunidades -----
UPDATE oportunidades o
SET score = fn_score_oportunidade(e.valor_total_devida, e.capital_social, e.cnpj_situacao, o.motor, e.tributo_principal),
    atualizado_em = now()
FROM empresas e WHERE e.cnpj_raiz = o.cnpj_raiz;

-- ============================================================
-- ROLLBACK:
--   volte a função para a assinatura de 4 args (migration 30d)
--   e recrie a view sem tributo_principal/ratio; depois:
--   ALTER TABLE empresas DROP COLUMN IF EXISTS tributo_principal;
--   (config_score mantém as colunas novas sem efeito colateral)
-- ============================================================
