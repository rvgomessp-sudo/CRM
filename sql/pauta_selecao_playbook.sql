-- ============================================================
-- SELEÇÃO DA PAUTA PELO PLAYBOOK (Rodrigo, 2026-08-19)
-- Classifica os teores DJEN (eventos.texto) nos 5 padrões de
-- oportunidade e monta a lista do dia. Rodar no SQL editor (ou
-- automatizar): o SELECT final alimenta INSERTs em `pauta`.
--
-- Padrões (ordem de prioridade da abordagem):
--   P2_GARANTIA_EXIGIDA  juiz indefere/manda garantir o juízo — excelente
--   P1_EMBARGOS          embargos à execução (exigem garantia) — excelente
--   P0_CONSTRICAO_FRESCA SISBAJUD/penhora recente — sufoco em curso
--   P3_PENHORA_IMOVEL    penhora de imóveis — trabalhar substituição
--   P4_APOLICE           já usa seguro garantia — aniversário/renovação
--   P5_NPJ               transação/NPJ descumprido — delicado
-- ============================================================

WITH ev AS (
  SELECT e.cnpj_raiz, e.numero_processo, e.tipo, e.ocorrido_em,
         left(regexp_replace(e.texto,'\s+',' ','g'), 320) AS trecho,
         e.advogados->0->>'nome' AS adv_nome, e.advogados->0->>'oab' AS adv_oab,
         e.link_publicacao,
         CASE
           WHEN e.texto ~* 'seguro[- ]garantia|ap[óo]lice' THEN 'P4_APOLICE'
           WHEN e.texto ~* '(transa[çc][ãa]o|neg[óo]cio jur[íi]dico)' AND e.texto ~* 'descumpr|rescis|inadimpl' THEN 'P5_NPJ'
           WHEN e.texto ~* 'penhora' AND e.texto ~* 'im[óo]ve|matr[íi]cula' THEN 'P3_PENHORA_IMOVEL'
           WHEN e.texto ~* 'garant(a|ir|ia) (o |do )?ju[íi]zo|indefiro|indeferid|cau[çc][ãa]o|suspens[ãa]o da exigibilidade' THEN 'P2_GARANTIA_EXIGIDA'
           WHEN e.tipo = 'EMBARGOS_EXEC' OR e.texto ~* 'embargos [àa] execu[çc][ãa]o' THEN 'P1_EMBARGOS'
           WHEN e.tipo IN ('SISBAJUD','PENHORA') AND e.ocorrido_em >= now()::date - 60 THEN 'P0_CONSTRICAO_FRESCA'
         END AS padrao
  FROM eventos e WHERE e.fonte='JUDICIAL' AND e.texto IS NOT NULL
),
prio AS (
  SELECT *, CASE padrao WHEN 'P2_GARANTIA_EXIGIDA' THEN 1 WHEN 'P1_EMBARGOS' THEN 2
    WHEN 'P0_CONSTRICAO_FRESCA' THEN 3 WHEN 'P3_PENHORA_IMOVEL' THEN 4
    WHEN 'P4_APOLICE' THEN 5 WHEN 'P5_NPJ' THEN 6 END AS pr
  FROM ev WHERE padrao IS NOT NULL
),
melhor AS (          -- melhor gatilho por empresa
  SELECT DISTINCT ON (cnpj_raiz) * FROM prio
  ORDER BY cnpj_raiz, pr, ocorrido_em DESC NULLS LAST
)
SELECT m.padrao, f.nome_devedor, f.uf_devedor, m.cnpj_raiz,
       round(f.valor_total_devida/1e6,1) AS divida_mi, f.zona_risco, round(f.score) AS score,
       m.numero_processo, m.tipo, m.ocorrido_em, m.adv_nome, m.adv_oab, m.trecho, m.link_publicacao
FROM melhor m
JOIN vw_fila_oportunidades f ON f.cnpj_raiz = m.cnpj_raiz
WHERE f.alvo_marinheiro = true
  -- não repetir quem já esteve em pauta nos últimos 7 dias:
  AND NOT EXISTS (SELECT 1 FROM pauta p WHERE p.cnpj_raiz = m.cnpj_raiz AND p.data >= now()::date - 7)
ORDER BY m.pr, m.ocorrido_em DESC NULLS LAST, f.score DESC NULLS LAST
LIMIT 20;  -- 10 por sócio (regra: mín. 5, alvo 10/dia cada)
