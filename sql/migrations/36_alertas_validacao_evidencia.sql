-- ============================================================
-- 36 · ALERTAS + VALIDAÇÃO HUMANA + CONDIÇÃO DE EVIDÊNCIA
-- Fase 2/3/4 da ordem executiva de 2026-08-19.
--
-- Materializa a regra central: EVENTO → ALERTA → VALIDAÇÃO HUMANA
-- → OPORTUNIDADE. Nada vira oportunidade sem passar por validação.
-- Nada vira fato sem condição de evidência declarada.
--
-- Aditiva e idempotente. NÃO APLICADA em produção até autorização
-- expressa (registrada no chat/aprovação). Rollback ao fim.
-- ============================================================

-- ---- Alertas: a unidade que o operador valida ----
CREATE TABLE IF NOT EXISTS alertas (
  id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  cnpj_raiz          char(8) NOT NULL,
  evento_id          uuid REFERENCES eventos(id),
  numero_processo    text,
  titulo             text NOT NULL,
  gravidade          text NOT NULL DEFAULT 'IMPORTANTE'
                     CHECK (gravidade IN ('CRITICO','IMPORTANTE','INFORMATIVO')),

  -- Estado operacional do alerta (pré-funil). O funil comercial
  -- (OPORTUNIDADE → … → FECHADO/PERDIDO) continua em oportunidades/empresas.
  estado             text NOT NULL DEFAULT 'CANDIDATO'
                     CHECK (estado IN ('CANDIDATO','AGUARDANDO_VALIDACAO','VALIDADO',
                                       'CONFLITO','DESCARTADO','MONITORAR')),

  -- Condição da evidência que sustenta o alerta (Fase 4 da ordem)
  evidencia_condicao text NOT NULL DEFAULT 'HIPOTESE'
                     CHECK (evidencia_condicao IN ('CONFIRMADO','CORROBORADO','HIPOTESE',
                                                   'LACUNA','CONFLITO','DESATUALIZADO',
                                                   'NAO_CONFIRMAVEL')),

  -- Papel processual da empresa NO PROCESSO DO ALERTA.
  -- Regra de aceite: credora nunca tratada como devedora.
  papel_processual   text NOT NULL DEFAULT 'NAO_CONFIRMADO'
                     CHECK (papel_processual IN ('DEVEDORA','CREDORA','AMBIGUO',
                                                 'PENDENTE','NAO_CONFIRMADO')),

  -- Ramo pelo dígito J do CNJ (4=Federal, 5=Trabalhista, 8=Estadual)
  ramo               text CHECK (ramo IN ('FEDERAL','TRABALHISTA','ESTADUAL','OUTRO')),

  -- Valor DO PROCESSO (quando extraível do teor). NUNCA é a dívida PGFN;
  -- a dívida vive em inscricoes/empresas e as telas devem rotular ambas.
  valor_processo     numeric,

  -- Pendências de validação: lista de códigos, ex.
  -- ["RETESTE_RECORRENCIA_PGFN","PAPEL_PROCESSUAL_PENDENTE","GRUPO_NAO_CONFIRMADO"]
  pendencias         jsonb NOT NULL DEFAULT '[]',

  -- Score legado (da pauta/fila) — HISTÓRICO, NÃO AUTORITATIVO (Fase 8)
  score_legado       numeric,

  -- Proveniência (Fase 4): de onde veio e o trecho local que sustenta
  fonte              text,            -- ex.: 'DJEN_COMUNICA', 'PAUTA_20_08', 'PGFN'
  link_fonte         text,
  trecho             text,

  padrao_playbook    text,            -- P0..P5, quando originado pelo playbook
  responsavel        text CHECK (responsavel IN ('RODRIGO','ANA')),

  detectado_em       timestamptz NOT NULL DEFAULT now(),
  validado_por       uuid REFERENCES profiles(id),
  validado_em        timestamptz,
  validacao_nota     text,
  criado_em          timestamptz NOT NULL DEFAULT now(),

  UNIQUE (cnpj_raiz, numero_processo, titulo)
);

CREATE INDEX IF NOT EXISTS idx_alertas_estado    ON alertas (estado, gravidade);
CREATE INDEX IF NOT EXISTS idx_alertas_cnpj      ON alertas (cnpj_raiz);
CREATE INDEX IF NOT EXISTS idx_alertas_detectado ON alertas (detectado_em DESC);

ALTER TABLE alertas ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS alertas_all ON alertas;
CREATE POLICY alertas_all ON alertas FOR ALL TO authenticated USING (true) WITH CHECK (true);

-- ---- Trilha de auditoria de decisões sobre alertas ----
CREATE TABLE IF NOT EXISTS alertas_auditoria (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  alerta_id   uuid NOT NULL REFERENCES alertas(id),
  de_estado   text,
  para_estado text NOT NULL,
  nota        text,
  autor       uuid REFERENCES profiles(id),
  em          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_alertas_aud ON alertas_auditoria (alerta_id, em DESC);
ALTER TABLE alertas_auditoria ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS alertas_aud_all ON alertas_auditoria;
CREATE POLICY alertas_aud_all ON alertas_auditoria FOR ALL TO authenticated USING (true) WITH CHECK (true);

-- Toda mudança de estado de alerta gera linha de auditoria
CREATE OR REPLACE FUNCTION fn_auditar_alerta() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path = public AS $$
BEGIN
  IF TG_OP = 'UPDATE' AND NEW.estado IS DISTINCT FROM OLD.estado THEN
    INSERT INTO alertas_auditoria (alerta_id, de_estado, para_estado, nota, autor)
    VALUES (NEW.id, OLD.estado, NEW.estado, NEW.validacao_nota, NEW.validado_por);
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS trg_auditar_alerta ON alertas;
CREATE TRIGGER trg_auditar_alerta AFTER UPDATE ON alertas
  FOR EACH ROW EXECUTE FUNCTION fn_auditar_alerta();

-- ---- Guarda de promoção: alerta só vira oportunidade se VALIDADO ----
-- (a promoção em si é ação humana na UI; esta função é o único caminho)
CREATE OR REPLACE FUNCTION fn_promover_alerta(p_alerta uuid, p_autor uuid)
RETURNS uuid LANGUAGE plpgsql SECURITY INVOKER SET search_path = public AS $$
DECLARE v alertas%ROWTYPE; v_op uuid;
BEGIN
  SELECT * INTO v FROM alertas WHERE id = p_alerta FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'alerta % não existe', p_alerta; END IF;
  IF v.estado <> 'VALIDADO' THEN
    RAISE EXCEPTION 'alerta % está %, promoção exige VALIDADO', p_alerta, v.estado;
  END IF;
  UPDATE oportunidades SET triagem = 'ABORDAR', atualizado_em = now()
   WHERE cnpj_raiz = v.cnpj_raiz RETURNING id INTO v_op;
  INSERT INTO alertas_auditoria (alerta_id, de_estado, para_estado, nota, autor)
  VALUES (p_alerta, 'VALIDADO', 'VALIDADO', 'promovido a oportunidade '||coalesce(v_op::text,'(sem fila)'), p_autor);
  RETURN v_op;
END $$;

-- ROLLBACK:
-- DROP FUNCTION IF EXISTS fn_promover_alerta(uuid, uuid);
-- DROP TRIGGER IF EXISTS trg_auditar_alerta ON alertas;
-- DROP FUNCTION IF EXISTS fn_auditar_alerta();
-- DROP TABLE IF EXISTS alertas_auditoria;
-- DROP TABLE IF EXISTS alertas;
