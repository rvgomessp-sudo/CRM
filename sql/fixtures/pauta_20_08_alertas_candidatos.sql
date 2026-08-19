-- ============================================================
-- FIXTURE AUDITÁVEL · Pauta de Prospecção 20/08 → 20 ALERTAS CANDIDATOS
-- Fase 2 da ordem executiva de 2026-08-19.
--
-- Fonte: tabela `pauta` (data 2026-08-20) + eventos (teor DJEN) + vw_fila.
-- Nenhum registro nasce como oportunidade confirmada. O score da fila
-- entra como score_legado (histórico, não autoritativo).
--
-- Detecções da ordem codificadas:
--  (1) RETESTE_RECORRENCIA_PGFN em TODOS os 20;
--  (2) Della Coletta 44691236 → papel PENDENTE;
--  (3) Maxlog 10447922 → papel CREDORA (evidência CONFLITO);
--  (4) AC Coelho 37083474 → papel CREDORA (evidência CONFLITO);
--  (5) Aguapeí 35203047 → GRUPO_NAO_CONFIRMADO (holding);
--  (6) meta-alerta INFORMATIVO da inconsistência "3 avisos" × 4 registros;
--  (7) valor_processo separado da dívida PGFN (LACUNA quando não extraído);
--  (8) ramo TRABALHISTA marcado ≠ dívida fiscal (pendência específica);
--  (9) Pandurata 70940994 → FONTE_ORIGINAL_AUSENTE (link genérico do DJE);
-- (10) estado inicial CANDIDATO → validação humana obrigatória.
--
-- Idempotente (ON CONFLICT DO NOTHING). Requer migration 36.
-- ============================================================

INSERT INTO alertas (
  cnpj_raiz, evento_id, numero_processo, titulo, gravidade, estado,
  evidencia_condicao, papel_processual, ramo, valor_processo, pendencias,
  score_legado, fonte, link_fonte, trecho, padrao_playbook, responsavel, detectado_em
)
SELECT
  p.cnpj_raiz,
  ev.id,
  p.numero_processo,
  left('Pauta 20/08 · ' || coalesce(nullif(regexp_replace(p.motivo, '^★ ', ''), ''), p.padrao), 200) AS titulo,
  CASE WHEN f.zona_risco = 'SUFOCO' THEN 'CRITICO' ELSE 'IMPORTANTE' END,
  'CANDIDATO',
  CASE
    WHEN p.cnpj_raiz IN ('10447922','37083474') THEN 'CONFLITO'   -- credoras na publicação usada
    WHEN p.cnpj_raiz IN ('44691236','35203047') THEN 'HIPOTESE'   -- papel/grupo pendentes
    WHEN ev.texto IS NOT NULL                   THEN 'CORROBORADO'-- teor DJEN sustenta
    ELSE 'LACUNA'
  END,
  CASE
    WHEN p.cnpj_raiz IN ('10447922','37083474') THEN 'CREDORA'
    WHEN p.cnpj_raiz = '44691236'               THEN 'PENDENTE'
    WHEN p.cnpj_raiz = '35203047'               THEN 'PENDENTE'
    WHEN ev.texto IS NOT NULL                   THEN 'DEVEDORA'
    ELSE 'NAO_CONFIRMADO'
  END,
  CASE substring(regexp_replace(p.numero_processo,'[^0-9]','','g') from 14 for 1)
    WHEN '4' THEN 'FEDERAL' WHEN '5' THEN 'TRABALHISTA' WHEN '8' THEN 'ESTADUAL' ELSE 'OUTRO' END,
  NULL::numeric,  -- valor do processo NÃO extraído ainda → LACUNA declarada em pendências
  (
    '["RETESTE_RECORRENCIA_PGFN","VALOR_PROCESSO_NAO_EXTRAIDO"]'::jsonb
    || CASE WHEN p.cnpj_raiz = '44691236' THEN '["PAPEL_PROCESSUAL_PENDENTE"]'::jsonb ELSE '[]'::jsonb END
    || CASE WHEN p.cnpj_raiz IN ('10447922','37083474') THEN '["EMPRESA_APARECE_COMO_CREDORA"]'::jsonb ELSE '[]'::jsonb END
    || CASE WHEN p.cnpj_raiz = '35203047' THEN '["GRUPO_ECONOMICO_NAO_CONFIRMADO"]'::jsonb ELSE '[]'::jsonb END
    || CASE WHEN p.cnpj_raiz = '70940994' THEN '["FONTE_ORIGINAL_AUSENTE"]'::jsonb ELSE '[]'::jsonb END
    || CASE WHEN substring(regexp_replace(p.numero_processo,'[^0-9]','','g') from 14 for 1) = '5'
            THEN '["EVENTO_TRABALHISTA_NAO_E_DIVIDA_FISCAL"]'::jsonb ELSE '[]'::jsonb END
    || CASE WHEN p.alerta IS NOT NULL THEN '["AVISO_DA_PAUTA_ORIGINAL"]'::jsonb ELSE '[]'::jsonb END
  ),
  f.score,                       -- score LEGADO (histórico, não autoritativo)
  'PAUTA_20_08+DJEN_COMUNICA',
  ev.link_publicacao,
  left(ev.texto, 400),
  p.padrao,
  p.responsavel,
  p.criado_em
FROM pauta p
LEFT JOIN eventos ev
  ON ev.fonte='JUDICIAL'
 AND regexp_replace(ev.numero_processo,'[^0-9]','','g') = regexp_replace(p.numero_processo,'[^0-9]','','g')
 AND ev.texto IS NOT NULL
LEFT JOIN vw_fila_oportunidades f ON f.cnpj_raiz = p.cnpj_raiz
WHERE p.data = '2026-08-20'
ON CONFLICT (cnpj_raiz, numero_processo, titulo) DO NOTHING;

-- Deduplicação: um alerta por linha da pauta (o LEFT JOIN de eventos pode
-- multiplicar por processo com vários eventos enriquecidos) — manter o 1º.
DELETE FROM alertas a USING alertas b
WHERE a.fonte = 'PAUTA_20_08+DJEN_COMUNICA' AND b.fonte = a.fonte
  AND a.cnpj_raiz = b.cnpj_raiz AND a.numero_processo = b.numero_processo
  AND a.id > b.id;

-- (6) Meta-alerta: inconsistência documental da pauta original
-- ("três cards têm aviso" no texto × registros com alerta preenchido)
INSERT INTO alertas (cnpj_raiz, titulo, gravidade, estado, evidencia_condicao,
                     papel_processual, pendencias, fonte, trecho)
SELECT '00000000',
       'Inconsistência documental · Pauta 20/08 declara 3 avisos; registros com alerta = ' || count(*),
       'INFORMATIVO', 'CANDIDATO', 'CONFIRMADO', 'NAO_CONFIRMADO',
       '["CORRIGIR_DOCUMENTO_PAUTA"]'::jsonb,
       'AUDITORIA_FASE_2',
       'Contagem verificável: SELECT count(*) FROM pauta WHERE data=''2026-08-20'' AND alerta IS NOT NULL'
FROM pauta WHERE data = '2026-08-20' AND alerta IS NOT NULL
HAVING count(*) <> 3
ON CONFLICT (cnpj_raiz, numero_processo, titulo) DO NOTHING;

-- Conferência da fixture (esperado: 20 candidatos + 1 informativo se a
-- inconsistência existir; 2 CREDORA; 2 PENDENTE; 12 CRITICO)
SELECT estado, gravidade, papel_processual, count(*)
FROM alertas WHERE fonte IN ('PAUTA_20_08+DJEN_COMUNICA','AUDITORIA_FASE_2')
GROUP BY 1,2,3 ORDER BY 1,2,3;
