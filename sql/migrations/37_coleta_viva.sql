-- ============================================================
-- 37 · COLETA VIVA — o batimento do monitor (Fase 15)
-- pg_cron agenda; Edge Function `coleta-djen` consulta o DJEN em
-- lotes; fn_rematerializar_sinais() reprocessa zona/score 1x/dia.
-- Automação integral permitida pela matriz da ordem: coleta,
-- normalização, dedup, extração, alerta interno. Promoção continua
-- humana (fn_promover_alerta).
-- Aditiva. Rollback ao fim.
-- ============================================================

CREATE EXTENSION IF NOT EXISTS pg_cron;
CREATE EXTENSION IF NOT EXISTS pg_net;

-- ---- Fila de monitoramento: quem o coletor vigia ----------------
CREATE TABLE IF NOT EXISTS processos_monitorados (
  numero_processo   text PRIMARY KEY,          -- 20 dígitos (normalizado)
  cnpj_raiz         char(8) NOT NULL,
  ativo             boolean NOT NULL DEFAULT true,
  ultima_checagem   timestamptz,
  checagens         int NOT NULL DEFAULT 0,
  novidades_total   int NOT NULL DEFAULT 0,
  criado_em         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_procmon_fila ON processos_monitorados (ativo, ultima_checagem NULLS FIRST);
ALTER TABLE processos_monitorados ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS procmon_all ON processos_monitorados;
CREATE POLICY procmon_all ON processos_monitorados FOR ALL TO authenticated USING (true) WITH CHECK (true);

-- Seed: todos os processos judiciais já conhecidos
INSERT INTO processos_monitorados (numero_processo, cnpj_raiz)
SELECT DISTINCT regexp_replace(numero_processo,'[^0-9]','','g'), cnpj_raiz
FROM eventos
WHERE fonte='JUDICIAL' AND numero_processo IS NOT NULL
  AND length(regexp_replace(numero_processo,'[^0-9]','','g')) = 20
ON CONFLICT (numero_processo) DO NOTHING;

-- ---- Log de coletas (alimenta o /monitoramento) -----------------
CREATE TABLE IF NOT EXISTS coletas (
  id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  fonte          text NOT NULL,               -- 'DJEN_COMUNICA'
  iniciada_em    timestamptz NOT NULL DEFAULT now(),
  concluida_em   timestamptz,
  processados    int DEFAULT 0,
  novos_eventos  int DEFAULT 0,
  novos_alertas  int DEFAULT 0,
  erros          int DEFAULT 0,
  detalhe        text
);
CREATE INDEX IF NOT EXISTS idx_coletas_fonte ON coletas (fonte, iniciada_em DESC);
ALTER TABLE coletas ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS coletas_all ON coletas;
CREATE POLICY coletas_all ON coletas FOR ALL TO authenticated USING (true) WITH CHECK (true);

-- ---- Rematerialização dos sinais (cópia fiel das migrations 33/34) ----
CREATE OR REPLACE FUNCTION fn_rematerializar_sinais() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
BEGIN
  -- sinal quente = só fiscal (não-trabalhista)  [migration 34 §4]
  UPDATE empresas SET evento_judicial_tipo=NULL, evento_judicial_em=NULL, evento_judicial_peso=NULL;
  UPDATE empresas e
  SET evento_judicial_tipo = h.tipo,
      evento_judicial_em   = h.dt,
      evento_judicial_peso = round(h.peso * CASE WHEN h.dt >= (CURRENT_DATE-365) THEN 1.0 ELSE 0.6 END, 2)
  FROM (
    SELECT DISTINCT ON (ev.cnpj_raiz)
      ev.cnpj_raiz, ev.tipo, ev.ocorrido_em AS dt,
      (c.evento_pesos ->> ev.tipo)::numeric AS peso
    FROM eventos ev CROSS JOIN config_score c
    WHERE ev.fonte='JUDICIAL' AND c.id=1
      AND COALESCE(ev.payload->>'ramo','OUTRO') <> 'TRABALHISTA'
    ORDER BY ev.cnpj_raiz, (c.evento_pesos ->> ev.tipo)::numeric DESC NULLS LAST, ev.ocorrido_em DESC
  ) h WHERE h.cnpj_raiz = e.cnpj_raiz;

  -- trabalhista: contador separado  [migration 34 §4]
  UPDATE empresas SET eventos_trabalhistas=NULL, evento_trabalhista_em=NULL;
  UPDATE empresas e
  SET eventos_trabalhistas=t.n, evento_trabalhista_em=t.max_d
  FROM (SELECT cnpj_raiz, count(*) n, max(ocorrido_em) max_d
        FROM eventos WHERE fonte='JUDICIAL' AND payload->>'ramo'='TRABALHISTA'
        GROUP BY cnpj_raiz) t
  WHERE t.cnpj_raiz = e.cnpj_raiz;

  -- zona de risco  [migration 34 §5]
  UPDATE empresas SET zona_risco = CASE
    WHEN evento_judicial_tipo IN ('SISBAJUD','PENHORA','RENAJUD')
         AND evento_judicial_em >= CURRENT_DATE - 180 THEN 'SUFOCO'
    WHEN evento_judicial_tipo IS NOT NULL OR motor IN ('A1','B1') THEN 'VERMELHA'
    ELSE 'AMARELA'
  END;

  -- score  [migration 34 §6]
  UPDATE oportunidades o
  SET score = fn_score_oportunidade(e.valor_total_devida, e.capital_social, e.cnpj_situacao,
                                    o.motor, e.tributo_principal, e.evento_judicial_peso),
      atualizado_em = now()
  FROM empresas e WHERE e.cnpj_raiz = o.cnpj_raiz;
END $$;
REVOKE EXECUTE ON FUNCTION fn_rematerializar_sinais() FROM anon, authenticated;

-- Helper do coletor: marca a checagem e acumula novidades (atômico)
CREATE OR REPLACE FUNCTION fn_incrementa_checagem(p_processo text, p_novidades int)
RETURNS void LANGUAGE sql SECURITY DEFINER SET search_path = public AS $$
  UPDATE processos_monitorados
  SET ultima_checagem = now(),
      checagens = checagens + 1,
      novidades_total = novidades_total + GREATEST(p_novidades, 0)
  WHERE numero_processo = p_processo;
$$;
REVOKE EXECUTE ON FUNCTION fn_incrementa_checagem(text, int) FROM anon, authenticated;

-- ---- Agendamentos ----------------------------------------------
-- Coleta DJEN: a cada 15 min, um lote de processos via Edge Function.
SELECT cron.schedule(
  'coleta-djen-15min', '*/15 * * * *',
  $$ SELECT net.http_post(
       url    := 'https://pnmasrynyiaqhxkcqygj.supabase.co/functions/v1/coleta-djen',
       headers:= '{"Content-Type":"application/json"}'::jsonb,
       body   := '{"batch":40}'::jsonb,
       timeout_milliseconds := 8000) $$
) WHERE NOT EXISTS (SELECT 1 FROM cron.job WHERE jobname='coleta-djen-15min');

-- Rematerialização: diária, 06:10 UTC (03:10 BRT)
SELECT cron.schedule(
  'rematerializa-sinais-diario', '10 6 * * *',
  $$ SELECT fn_rematerializar_sinais() $$
) WHERE NOT EXISTS (SELECT 1 FROM cron.job WHERE jobname='rematerializa-sinais-diario');

-- ROLLBACK:
-- SELECT cron.unschedule('coleta-djen-15min');
-- SELECT cron.unschedule('rematerializa-sinais-diario');
-- DROP FUNCTION IF EXISTS fn_rematerializar_sinais();
-- DROP TABLE IF EXISTS coletas;
-- DROP TABLE IF EXISTS processos_monitorados;
