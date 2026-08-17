-- ============================================================
-- MIGRATION 34 — F2: separação FISCAL × TRABALHISTA + zona de risco
-- Vazquez & Fonseca | CRM V3 (Supabase pnmasrynyiaqhxkcqygj)
--
-- "Não há qualquer relação no mundo securitário" (Rodrigo):
-- constrição TRABALHISTA (TRT/TST) não gatilha seguro garantia TRIBUTÁRIO.
-- Este passo:
--   1. classifica o RAMO de cada evento judicial pelo dígito J do nº CNJ
--      (NNNNNNN-DD.AAAA.J.TR.OOOO → J: 4=Federal, 5=Trabalhista, 8=Estadual)
--   2. o sinal quente (evento_judicial_*) passa a considerar SÓ o não-trabalhista
--   3. trabalhista vira contador separado (sinal de estresse de caixa, informativo)
--   4. FILTRO fora-de-perfil: exclui seguradoras/resseguradoras/capitalização,
--      bancos/corretoras de títulos, operadoras de planos, entes públicos (CONAB)
--      e empresas em liquidação/RJ/massa falida — não são tomadores possíveis
--   5. ZONA DE RISCO por empresa: SUFOCO (constrição fiscal recente) >
--      VERMELHA (execução ajuizada / evento fiscal) > AMARELA (só dívida ativa)
--
-- Aditiva e reversível (exclusões usam excluido+motivo_exclusao; basta reverter).
-- ============================================================

-- ---- 1. RAMO nos eventos judiciais (dígito J do número CNJ) ----
UPDATE eventos SET payload = payload || jsonb_build_object('ramo',
  CASE
    WHEN numero_processo ~ '\.\d{4}\.5\.' THEN 'TRABALHISTA'
    WHEN numero_processo ~ '\.\d{4}\.4\.' THEN 'FEDERAL'
    WHEN numero_processo ~ '\.\d{4}\.8\.' THEN 'ESTADUAL'
    WHEN numero_processo ~ '^\d{20}$' AND substring(numero_processo from 14 for 1) = '5' THEN 'TRABALHISTA'
    WHEN numero_processo ~ '^\d{20}$' AND substring(numero_processo from 14 for 1) = '4' THEN 'FEDERAL'
    WHEN numero_processo ~ '^\d{20}$' AND substring(numero_processo from 14 for 1) = '8' THEN 'ESTADUAL'
    ELSE 'OUTRO'
  END)
WHERE fonte = 'JUDICIAL' AND numero_processo IS NOT NULL;

-- ---- 2. FILTRO fora-de-perfil (o "filtro que escapa") -----------
UPDATE empresas SET excluido = true,
  motivo_exclusao = 'FORA DE PERFIL: instituição financeira/seguradora'
WHERE NOT excluido AND (
     nome_devedor ILIKE '%SEGURADORA%'  OR nome_devedor ILIKE '%RESSEGURA%'
  OR nome_devedor ILIKE '% SEGUROS%'    OR nome_devedor ILIKE '%CAPITALIZACAO%'
  OR nome_devedor ILIKE 'BANCO %'       OR nome_devedor ILIKE '%CORRETORA DE TITULOS%'
  OR nome_devedor ILIKE '%CORRETORA DE SEGUROS%'
  OR nome_devedor ILIKE '%OPERADORA DE PLANOS%'
  OR nome_devedor ILIKE '%PREVIDENCIA PRIVADA%'
);

UPDATE empresas SET excluido = true,
  motivo_exclusao = 'FORA DE PERFIL: ente/empresa pública'
WHERE NOT excluido AND (
     nome_devedor ILIKE '%CONAB%'
  OR nome_devedor ILIKE 'COMPANHIA NACIONAL DE ABASTECIMENTO%'
);

UPDATE empresas SET excluido = true,
  motivo_exclusao = 'FORA DE PERFIL: liquidação/recuperação/massa falida'
WHERE NOT excluido AND (
     nome_devedor ILIKE '%EM LIQUIDACAO%'
  OR nome_devedor ILIKE '%RECUPERACAO JUDICIAL%'
  OR nome_devedor ILIKE '%MASSA FALIDA%'
);

-- ---- 3. Colunas novas ------------------------------------------
ALTER TABLE empresas ADD COLUMN IF NOT EXISTS zona_risco            TEXT;
ALTER TABLE empresas ADD COLUMN IF NOT EXISTS eventos_trabalhistas  INT;
ALTER TABLE empresas ADD COLUMN IF NOT EXISTS evento_trabalhista_em DATE;

-- ---- 4. Sinal quente = SÓ FISCAL (não-trabalhista) --------------
UPDATE empresas SET evento_judicial_tipo = NULL, evento_judicial_em = NULL, evento_judicial_peso = NULL;

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
    AND COALESCE(ev.payload->>'ramo','OUTRO') <> 'TRABALHISTA'
  ORDER BY ev.cnpj_raiz, (c.evento_pesos ->> ev.tipo)::numeric DESC NULLS LAST, ev.ocorrido_em DESC
) h
WHERE h.cnpj_raiz = e.cnpj_raiz;

-- trabalhista: contador separado (informativo, fora do score)
UPDATE empresas SET eventos_trabalhistas = NULL, evento_trabalhista_em = NULL;
UPDATE empresas e
SET eventos_trabalhistas = t.n, evento_trabalhista_em = t.max_d
FROM (
  SELECT cnpj_raiz, count(*) AS n, max(ocorrido_em) AS max_d
  FROM eventos WHERE fonte='JUDICIAL' AND payload->>'ramo' = 'TRABALHISTA'
  GROUP BY cnpj_raiz
) t
WHERE t.cnpj_raiz = e.cnpj_raiz;

-- ---- 5. ZONA DE RISCO ------------------------------------------
-- SUFOCO: constrição FISCAL recente (SISBAJUD/penhora/RENAJUD ≤180d)
-- VERMELHA: execução ajuizada (motor A1/B1) ou qualquer evento fiscal achado
-- AMARELA: inscrita em dívida ativa, sem execução localizada
UPDATE empresas SET zona_risco = CASE
  WHEN evento_judicial_tipo IN ('SISBAJUD','PENHORA','RENAJUD')
       AND evento_judicial_em >= CURRENT_DATE - 180 THEN 'SUFOCO'
  WHEN evento_judicial_tipo IS NOT NULL OR motor IN ('A1','B1') THEN 'VERMELHA'
  ELSE 'AMARELA'
END;

-- ---- 6. Recompute score + view ---------------------------------
UPDATE oportunidades o
SET score = fn_score_oportunidade(e.valor_total_devida, e.capital_social, e.cnpj_situacao,
                                  o.motor, e.tributo_principal, e.evento_judicial_peso),
    atualizado_em = now()
FROM empresas e WHERE e.cnpj_raiz = o.cnpj_raiz;

CREATE OR REPLACE VIEW vw_fila_oportunidades
WITH (security_invoker = on) AS
SELECT
  o.id                AS oportunidade_id,
  o.cnpj_raiz, e.cnpj_completo, e.nome_devedor, e.uf_devedor,
  o.fonte, o.motor, o.estagio, o.triagem, o.triado_em, o.motivo_descarte,
  e.qtd_inscricoes, e.valor_total_devida, e.valor_maior_inscricao,
  e.capital_social, e.cnpj_situacao, e.tributo_principal,
  e.evento_judicial_tipo, e.evento_judicial_em,
  round(e.valor_total_devida / NULLIF(e.capital_social,0), 1) AS ratio_divida_capital,
  e.seguradora_alvo, e.prioridade,
  fn_score_oportunidade(e.valor_total_devida, e.capital_social, e.cnpj_situacao,
                        o.motor, e.tributo_principal, e.evento_judicial_peso) AS score,
  (e.cnpj_situacao = 'ATIVA'
    AND e.valor_total_devida >= (SELECT divida_min  FROM config_score WHERE id=1)
    AND e.capital_social BETWEEN (SELECT capital_min FROM config_score WHERE id=1)
                             AND (SELECT capital_max FROM config_score WHERE id=1)) AS alvo_marinheiro,
  e.zona_risco, e.eventos_trabalhistas, e.evento_trabalhista_em
FROM oportunidades o
JOIN empresas e ON e.cnpj_raiz = o.cnpj_raiz
WHERE e.ativo AND NOT e.excluido
ORDER BY score DESC NULLS LAST, e.valor_total_devida DESC;

-- ROLLBACK: reverter exclusões (excluido=false where motivo_exclusao like 'FORA DE PERFIL%'),
-- repopular evento_judicial_* sem o filtro de ramo, e dropar as 3 colunas novas.
