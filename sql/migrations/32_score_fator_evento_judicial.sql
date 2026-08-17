-- ============================================================
-- MIGRATION 32 — Fator EVENTO judicial no score
-- Vazquez & Fonseca | CRM V3 (Supabase pnmasrynyiaqhxkcqygj)
--
-- Liga o judiciário ao score: um alvo com evento de garantia recente
-- (anulatória/MS/embargos/cautelar/execução fiscal) sobe na fila.
-- A fila passa a responder "é bom alvo E está com garantia em demanda AGORA?".
--
-- Aditiva e reversível. Depende dos eventos JUDICIAL já carregados.
-- ============================================================

-- ---- 1. sinal judicial materializado por empresa ------------
ALTER TABLE empresas ADD COLUMN IF NOT EXISTS evento_judicial_tipo TEXT;
ALTER TABLE empresas ADD COLUMN IF NOT EXISTS evento_judicial_em   DATE;
ALTER TABLE empresas ADD COLUMN IF NOT EXISTS evento_judicial_peso NUMERIC;

-- ---- 2. config: pesos de evento + rebalanceio --------------
ALTER TABLE config_score ADD COLUMN IF NOT EXISTS peso_evento   INT NOT NULL DEFAULT 25;
ALTER TABLE config_score ADD COLUMN IF NOT EXISTS evento_pesos  JSONB NOT NULL DEFAULT
  '{"ANULATORIA":25,"MANDADO_SEGURANCA":22,"EMBARGOS_EXEC":20,"CAUTELAR":20,"EXECUCAO_FISCAL":16}'::jsonb;

-- base agora soma 75; evento entra com até 25 (total 100)
UPDATE config_score SET
  peso_viabilidade = 30,
  peso_porte       = 13,
  peso_divida      = 9,
  peso_situacao    = 8,
  motor_pesos      = '{"A1":15,"B1":12,"B2":10,"A2":8,"B3":7,"B4":6,"B5":5}'::jsonb,
  atualizado_em    = now()
WHERE id = 1;

-- ---- 3. materializa o evento judicial mais "quente" por empresa ----
-- (maior peso de classe; desempate pelo mais recente; recência decai após 1 ano)
UPDATE empresas e
SET evento_judicial_tipo = h.tipo,
    evento_judicial_em   = h.dt,
    evento_judicial_peso = round(h.peso * CASE WHEN h.dt >= (CURRENT_DATE - 365) THEN 1.0 ELSE 0.6 END, 2)
FROM (
  SELECT DISTINCT ON (ev.cnpj_raiz)
    ev.cnpj_raiz, ev.tipo, ev.ocorrido_em AS dt,
    (c.evento_pesos ->> ev.tipo)::numeric AS peso
  FROM eventos ev CROSS JOIN config_score c
  WHERE ev.fonte = 'JUDICIAL' AND c.id = 1
  ORDER BY ev.cnpj_raiz, (c.evento_pesos ->> ev.tipo)::numeric DESC NULLS LAST, ev.ocorrido_em DESC
) h
WHERE h.cnpj_raiz = e.cnpj_raiz;

-- ---- 4. função de score (agora com evento judicial) ---------
DROP VIEW IF EXISTS vw_fila_oportunidades;
DROP FUNCTION IF EXISTS fn_score_oportunidade(numeric, numeric, text, motor_tipo, text);

CREATE FUNCTION fn_score_oportunidade(
  p_valor_total  NUMERIC,
  p_capital      NUMERIC,
  p_situacao     TEXT,
  p_motor        motor_tipo,
  p_tributo      TEXT,
  p_evento_peso  NUMERIC
) RETURNS NUMERIC
LANGUAGE sql STABLE
SET search_path = public
AS $$
  SELECT GREATEST(0, ROUND((
      CASE WHEN p_valor_total >= c.divida_min
           THEN LEAST(c.peso_divida::numeric,
                      (c.peso_divida * ln(p_valor_total / c.divida_min + 1) / ln(50))::numeric)
           ELSE 0 END
    + CASE
        WHEN p_capital IS NULL OR p_capital <= 0 THEN (c.peso_viabilidade * 0.4)::numeric
        ELSE c.peso_viabilidade * GREATEST(0::numeric, LEAST(1::numeric,
               ((c.ratio_max - (p_valor_total / p_capital)) /
                NULLIF(c.ratio_max - c.ratio_ideal, 0))::numeric))
      END
    + COALESCE((c.tributo_pesos ->> COALESCE(p_tributo,'OUTROS'))::numeric,
               (c.tributo_pesos ->> 'OUTROS')::numeric)
    + CASE WHEN p_situacao = 'ATIVA' THEN c.peso_situacao::numeric ELSE 0 END
    + COALESCE((c.motor_pesos ->> p_motor::text)::numeric, 0)
    + LEAST(c.peso_evento::numeric, COALESCE(p_evento_peso, 0))   -- EVENTO judicial
  )::numeric, 2))
  FROM config_score c WHERE c.id = 1;
$$;

-- ---- 5. view da fila (expõe o evento judicial) --------------
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
  e.evento_judicial_tipo,
  e.evento_judicial_em,
  round(e.valor_total_devida / NULLIF(e.capital_social,0), 1) AS ratio_divida_capital,
  e.seguradora_alvo,
  e.prioridade,
  fn_score_oportunidade(e.valor_total_devida, e.capital_social, e.cnpj_situacao,
                        o.motor, e.tributo_principal, e.evento_judicial_peso) AS score,
  (e.cnpj_situacao = 'ATIVA'
    AND e.valor_total_devida >= (SELECT divida_min  FROM config_score WHERE id=1)
    AND e.capital_social BETWEEN (SELECT capital_min FROM config_score WHERE id=1)
                             AND (SELECT capital_max FROM config_score WHERE id=1)) AS alvo_marinheiro
FROM oportunidades o
JOIN empresas e ON e.cnpj_raiz = o.cnpj_raiz
WHERE e.ativo AND NOT e.excluido
ORDER BY score DESC NULLS LAST, e.valor_total_devida DESC;

-- ---- 6. recomputa score materializado ----------------------
UPDATE oportunidades o
SET score = fn_score_oportunidade(e.valor_total_devida, e.capital_social, e.cnpj_situacao,
                                  o.motor, e.tributo_principal, e.evento_judicial_peso),
    atualizado_em = now()
FROM empresas e WHERE e.cnpj_raiz = o.cnpj_raiz;
