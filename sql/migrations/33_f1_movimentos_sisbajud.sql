-- ============================================================
-- MIGRATION 33 — F1: movimentos judiciais (SISBAJUD/penhora/RENAJUD/citação)
-- Vazquez & Fonseca | CRM V3 (Supabase pnmasrynyiaqhxkcqygj)
--
-- O SISBAJUD (1º bloqueio de conta) é o "momento de sufoco" — maior sinal de
-- alerta. Detectado no texto das comunicações do DJEN. Este passo:
--   1. adiciona pesos de movimento em config_score.evento_pesos (SISBAJUD no topo)
--   2. re-materializa empresas.evento_judicial_* (SISBAJUD passa a ser o "quente")
--   3. recomputa oportunidades.score
--
-- Rodar DEPOIS de carregar os eventos de movimento (fonte JUDICIAL, tipo
-- SISBAJUD/PENHORA/RENAJUD/CITACAO). Aditiva e reversível.
-- ============================================================

-- 1. pesos de movimento (SISBAJUD = topo; citação = cronômetro, menor)
UPDATE config_score SET
  evento_pesos = evento_pesos || '{"SISBAJUD":25,"PENHORA":23,"RENAJUD":21,"CITACAO":12}'::jsonb,
  atualizado_em = now()
WHERE id = 1;

-- 2. re-materializa o evento judicial mais "quente" por empresa
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

-- 3. recomputa score materializado
UPDATE oportunidades o
SET score = fn_score_oportunidade(e.valor_total_devida, e.capital_social, e.cnpj_situacao,
                                  o.motor, e.tributo_principal, e.evento_judicial_peso),
    atualizado_em = now()
FROM empresas e WHERE e.cnpj_raiz = o.cnpj_raiz;

-- ROLLBACK: remover chaves de movimento do evento_pesos e re-materializar/recompute.
