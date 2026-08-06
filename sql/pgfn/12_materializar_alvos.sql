-- ============================================================
-- REMATERIALIZAR crm_alvos_nr1 COMPLETA
-- Inclui TODAS as colunas (TIPO_PESSOA, TIPO_DEVEDOR incluídos)
-- Aplica exclusões: GARANTIA na situação · SISPAR · Pessoa física
-- ============================================================

DROP TABLE IF EXISTS crm_alvos_nr1;

CREATE TABLE crm_alvos_nr1 AS
WITH contagem AS (
  SELECT REGEXP_REPLACE("CPF_CNPJ",'[^0-9]','','g') AS cnpj_limpo, COUNT(*) AS qtd
  FROM arquivo_lai_sida
  WHERE "TIPO_DEVEDOR" = 'PRINCIPAL' AND "TIPO_PESSOA" ILIKE '%jur%'
    AND LENGTH(REGEXP_REPLACE("CPF_CNPJ",'[^0-9]','','g')) = 14
  GROUP BY cnpj_limpo
),
unicos AS (SELECT cnpj_limpo FROM contagem WHERE qtd = 1)
SELECT
  -- todas as colunas originais na ordem de leitura definida:
  a."CPF_CNPJ",
  a."TIPO_PESSOA",
  a."TIPO_DEVEDOR",
  a."NOME_DEVEDOR",
  a."DATA_INSCRICAO",
  a."VALOR_CONSOLIDADO"::numeric                         AS valor_consolidado,
  a."SITUACAO_INSCRICAO",
  a."TIPO_SITUACAO_INSCRICAO",
  a."NUMERO_INSCRICAO",
  a."UF_DEVEDOR",
  a."RECEITA_PRINCIPAL",
  a."UNIDADE_RESPONSAVEL",
  a."INDICADOR_AJUIZADO",
  -- derivados p/ CRM:
  REGEXP_REPLACE(a."CPF_CNPJ",'[^0-9]','','g')           AS cnpj_limpo,
  LEFT(REGEXP_REPLACE(a."CPF_CNPJ",'[^0-9]','','g'),8)   AS cnpj_raiz,
  TO_DATE(a."DATA_INSCRICAO",'DD/MM/YYYY')               AS data_inscricao_iso,
  EXTRACT(YEAR FROM TO_DATE(a."DATA_INSCRICAO",'DD/MM/YYYY'))::int AS ano_inscricao,
  'A1'::text                                             AS motor,
  'ALTA'::text                                           AS prioridade,
  CASE
    WHEN a."VALOR_CONSOLIDADO"::numeric <= 20000000 THEN 'SANCOR'
    WHEN a."VALOR_CONSOLIDADO"::numeric <= 30000000 THEN 'BERKLEY'
    ELSE 'ZURICH'
  END                                                   AS seguradora_alvo
FROM arquivo_lai_sida a
JOIN unicos u ON u.cnpj_limpo = REGEXP_REPLACE(a."CPF_CNPJ",'[^0-9]','','g')
WHERE a."TIPO_DEVEDOR" = 'PRINCIPAL'
  AND a."TIPO_PESSOA" ILIKE '%jur%'          -- só PJ (exclui pessoa física)
  AND a."TIPO_SITUACAO_INSCRICAO" <> 'Garantia'
  AND UPPER(a."INDICADOR_AJUIZADO") = 'SIM'
  -- NOVO: exclui quem já tem GARANTIA na situação
  AND UPPER(a."SITUACAO_INSCRICAO") NOT LIKE '%GARANTIA%'
  -- NOVO: exclui quem já está NEGOCIADA NO SISPAR (parcelamento)
  AND UPPER(a."SITUACAO_INSCRICAO") NOT LIKE '%SISPAR%'
  AND UPPER(a."NOME_DEVEDOR") NOT LIKE '%RECUPERACAO%'
  AND UPPER(a."NOME_DEVEDOR") NOT LIKE '%FALIDO%'
  AND UPPER(a."NOME_DEVEDOR") NOT LIKE '%MASSA FALIDA%'
  AND UPPER(a."NOME_DEVEDOR") NOT LIKE 'MUNICIPIO%'
  AND UPPER(a."NOME_DEVEDOR") NOT LIKE '%PREFEITURA%'
  AND UPPER(a."NOME_DEVEDOR") NOT LIKE '%ASSOCIACAO%'
  AND UPPER(a."NOME_DEVEDOR") NOT LIKE '%FUNDACAO%'
  AND UPPER(a."NOME_DEVEDOR") NOT LIKE '%SINDICATO%'
  AND UPPER(a."NOME_DEVEDOR") NOT LIKE '%INSTITUTO%'
  AND UPPER(a."NOME_DEVEDOR") NOT LIKE '%ESTADO DE %'
  AND UPPER(a."NOME_DEVEDOR") NOT LIKE '%CAMARA MUNICIPAL%'
  AND UPPER(a."NOME_DEVEDOR") NOT LIKE '%SOCIEDADE%ASSISTENCIA%'
  AND a."RECEITA_PRINCIPAL" NOT ILIKE '%Simples Nacional%'
  AND TO_DATE(a."DATA_INSCRICAO",'DD/MM/YYYY') >= DATE '2023-01-01'
  AND a."VALOR_CONSOLIDADO"::numeric >= 1000000;

\echo '===== crm_alvos_nr1 RECRIADA (completa + exclusões GARANTIA/SISPAR/PF) ====='
SELECT COUNT(*) AS total_alvos,
       ROUND(SUM(valor_consolidado)/1000000,1) AS pipeline_mm
FROM crm_alvos_nr1;

\echo ''
\echo '===== Lista final para conferencia ====='
SELECT
  LEFT("NOME_DEVEDOR",40) AS devedor,
  "TIPO_PESSOA" AS tipo,
  "UF_DEVEDOR" AS uf,
  ROUND(valor_consolidado/1000000,2) AS mm,
  LEFT("SITUACAO_INSCRICAO",30) AS situacao
FROM crm_alvos_nr1
ORDER BY valor_consolidado DESC;
