-- ============================================================
-- CRM V3 Vazquez & Fonseca
-- Schema Supabase/PostgreSQL
-- Gerado: 2026-08-04
-- ============================================================

-- EXTENSÕES
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- busca fuzzy em nomes

-- ============================================================
-- TIPOS ENUMERADOS
-- ============================================================

CREATE TYPE pipeline_stage AS ENUM (
  'base_pgfn',
  'enriquecimento',
  'abordagem',
  'interesse_manifesto',
  'analise_rapida',
  'proposta_enviada',
  'submetido_sancor',
  'aprovado',
  'fechado',
  'receita_realizada'
);

CREATE TYPE prioridade_tipo    AS ENUM ('ALTA', 'MEDIA', 'BAIXA');
CREATE TYPE motor_tipo         AS ENUM ('A1', 'A2', 'B1', 'B2');
CREATE TYPE seguradora_tipo    AS ENUM ('SANCOR', 'BERKLEY', 'ZURICH', 'SWISS', 'CHUBB');
CREATE TYPE faixa_tipo         AS ENUM ('F1_SANCOR', 'F2_AMPLIADA');
CREATE TYPE papel_usuario      AS ENUM ('admin', 'operador');
CREATE TYPE canal_interacao    AS ENUM ('EMAIL', 'TELEFONE', 'WHATSAPP', 'REUNIAO', 'SISTEMA');
CREATE TYPE status_consulta    AS ENUM ('PENDENTE', 'APROVADO', 'RECUSADO', 'CONDICIONAL');
CREATE TYPE status_proposta    AS ENUM ('RASCUNHO', 'ENVIADA', 'EM_ANALISE', 'APROVADA', 'RECUSADA', 'CONVERTIDA');

-- ============================================================
-- PROFILES — estende auth.users do Supabase
-- ============================================================

CREATE TABLE profiles (
  id            UUID         REFERENCES auth.users(id) ON DELETE CASCADE PRIMARY KEY,
  nome          TEXT         NOT NULL,
  email         TEXT         NOT NULL UNIQUE,
  papel         papel_usuario DEFAULT 'operador',
  ativo         BOOLEAN      DEFAULT TRUE,
  criado_em     TIMESTAMPTZ  DEFAULT NOW(),
  atualizado_em TIMESTAMPTZ  DEFAULT NOW()
);

-- ============================================================
-- EMPRESAS
-- ============================================================

CREATE TABLE empresas (
  -- Identificação principal
  cnpj_raiz          CHAR(8)          PRIMARY KEY,    -- 8 dígitos, sem formatação
  cnpj_completo      TEXT             NOT NULL,
  nome_devedor       TEXT             NOT NULL,
  uf_devedor         CHAR(2),

  -- Classificação PGFN
  qtd_inscricoes     INT              DEFAULT 0,
  prioridade         prioridade_tipo,
  motor              motor_tipo,                       -- motor de MAIOR urgência
  faixa              faixa_tipo,

  -- Resumo financeiro (calculado na importação)
  valor_total_devida       NUMERIC(16,2)  DEFAULT 0,
  valor_maior_inscricao    NUMERIC(16,2)  DEFAULT 0,

  -- Pipeline comercial
  estagio              pipeline_stage   DEFAULT 'base_pgfn',
  seguradora_alvo      seguradora_tipo  DEFAULT 'SANCOR',
  responsavel_id       UUID             REFERENCES profiles(id),

  -- Status operacional
  ativo              BOOLEAN          DEFAULT TRUE,
  excluido           BOOLEAN          DEFAULT FALSE,
  motivo_exclusao    TEXT,

  -- Enriquecimento cadastral (preenchido manualmente ou via API)
  capital_social     NUMERIC(16,2),
  receita_estimada   NUMERIC(16,2),
  pl_estimado        NUMERIC(16,2),
  regime_tributario  TEXT,                             -- Lucro Real / Presumido / Simples
  cnpj_situacao      TEXT,                             -- ATIVA / INAPTA / BAIXADA
  cnae_principal     TEXT,
  segmento           TEXT,

  -- Decisor
  decisor_nome       TEXT,
  decisor_cargo      TEXT,
  decisor_email      TEXT,
  decisor_telefone   TEXT,
  decisor_linkedin   TEXT,

  -- Score V&F (0-100)
  score_vf           SMALLINT         CHECK (score_vf BETWEEN 0 AND 100),
  notas              TEXT,

  -- SLA / próxima ação
  ultimo_contato_em         TIMESTAMPTZ,
  proxima_acao_em           TIMESTAMPTZ,
  proxima_acao_descricao    TEXT,

  -- Proteção anti-desvio de corretagem
  nda_assinado       BOOLEAN          DEFAULT FALSE,
  nda_data           DATE,

  -- Metadados
  importado_em       TIMESTAMPTZ      DEFAULT NOW(),
  criado_em          TIMESTAMPTZ      DEFAULT NOW(),
  atualizado_em      TIMESTAMPTZ      DEFAULT NOW(),
  atualizado_por     UUID             REFERENCES profiles(id)
);

-- ============================================================
-- INSCRIÇÕES — 1 empresa : N inscrições
-- ============================================================

CREATE TABLE inscricoes (
  id                    UUID         DEFAULT uuid_generate_v4() PRIMARY KEY,

  -- FK empresa
  cnpj_raiz             CHAR(8)      NOT NULL REFERENCES empresas(cnpj_raiz) ON DELETE CASCADE,

  -- Dados originais do CSV PGFN
  cnpj_completo         TEXT         NOT NULL,
  nome_devedor          TEXT,
  uf_devedor            CHAR(2),
  numero_inscricao      TEXT         NOT NULL UNIQUE,   -- dedup key
  situacao_inscricao    TEXT,
  tipo_garantia         TEXT,
  flag_garantia         TEXT,
  tributo               TEXT,                           -- IRPJ / PIS / COFINS
  receita_principal     TEXT,
  data_inscricao        DATE,
  dias_inscricao        INT,
  ano_inscricao         SMALLINT,
  valor_brl             TEXT,
  valor_numerico        NUMERIC(16,2),
  indicador_ajuizado    BOOLEAN      DEFAULT FALSE,
  unidade_responsavel   TEXT,

  -- Classificação V&F
  motor                 motor_tipo,
  prioridade            prioridade_tipo,

  -- Metadados
  criado_em             TIMESTAMPTZ  DEFAULT NOW()
);

-- ============================================================
-- INTERAÇÕES — timeline de contatos
-- ============================================================

CREATE TABLE interacoes (
  id                    UUID           DEFAULT uuid_generate_v4() PRIMARY KEY,
  cnpj_raiz             CHAR(8)        NOT NULL REFERENCES empresas(cnpj_raiz) ON DELETE CASCADE,

  canal                 canal_interacao NOT NULL,
  resumo                TEXT           NOT NULL,
  proxima_acao          TEXT,
  proxima_acao_em       TIMESTAMPTZ,
  responsavel_id        UUID           REFERENCES profiles(id),
  estagio_na_interacao  pipeline_stage,

  criado_em             TIMESTAMPTZ    DEFAULT NOW(),
  criado_por            UUID           REFERENCES profiles(id)
);

-- ============================================================
-- CONSULTAS SEGURADORA
-- ============================================================

CREATE TABLE consultas_seguradora (
  id                UUID           DEFAULT uuid_generate_v4() PRIMARY KEY,
  cnpj_raiz         CHAR(8)        NOT NULL REFERENCES empresas(cnpj_raiz) ON DELETE CASCADE,

  seguradora        seguradora_tipo NOT NULL,
  status            status_consulta DEFAULT 'PENDENTE',
  limite_aprovado   NUMERIC(16,2),
  taxa_indicativa   NUMERIC(6,4),                    -- ex: 0.0050 = 0,50% a.a.
  modalidade        TEXT,                             -- Bid / Performance / Judicial Fiscal

  notas             TEXT,
  data_consulta     DATE           DEFAULT CURRENT_DATE,
  validade_ate      DATE,

  criado_em         TIMESTAMPTZ    DEFAULT NOW(),
  criado_por        UUID           REFERENCES profiles(id)
);

-- ============================================================
-- PROPOSTAS — output do VF Solver
-- ============================================================

CREATE TABLE propostas (
  id                    UUID           DEFAULT uuid_generate_v4() PRIMARY KEY,
  cnpj_raiz             CHAR(8)        NOT NULL REFERENCES empresas(cnpj_raiz) ON DELETE CASCADE,

  -- Operação
  valor_garantia        NUMERIC(16,2)  NOT NULL,
  inscricoes_cobertas   TEXT[],                       -- array de NUMERO_INSCRICAO
  seguradora            seguradora_tipo NOT NULL,

  -- Precificação (VF Solver)
  taxa_anual            NUMERIC(6,4)   NOT NULL,       -- 0.0050 = 0,50% a.a.
  prazo_anos            NUMERIC(4,2)   DEFAULT 1,
  premio_bruto          NUMERIC(16,2)  NOT NULL,
  comissao_pct          NUMERIC(5,4),                  -- 0.2000 = 20%
  comissao_valor        NUMERIC(16,2),
  honorarios_valor      NUMERIC(16,2)  DEFAULT 0,
  receita_vf_total      NUMERIC(16,2),

  -- Regra econômica: comissão + honorários > prêmio_bruto * threshold
  regra_economica_ok    BOOLEAN,

  -- Status
  status                status_proposta DEFAULT 'RASCUNHO',
  data_envio            DATE,
  validade_proposta     DATE,
  notas                 TEXT,
  pdf_url               TEXT,

  criado_em             TIMESTAMPTZ    DEFAULT NOW(),
  criado_por            UUID           REFERENCES profiles(id),
  atualizado_em         TIMESTAMPTZ    DEFAULT NOW()
);

-- ============================================================
-- HISTÓRICO DE MUDANÇAS DE ESTÁGIO
-- ============================================================

CREATE TABLE historico_estagios (
  id                UUID           DEFAULT uuid_generate_v4() PRIMARY KEY,
  cnpj_raiz         CHAR(8)        NOT NULL REFERENCES empresas(cnpj_raiz) ON DELETE CASCADE,

  estagio_anterior  pipeline_stage,
  estagio_novo      pipeline_stage NOT NULL,

  mudado_por        UUID           REFERENCES profiles(id),
  mudado_em         TIMESTAMPTZ    DEFAULT NOW(),
  observacao        TEXT
);

-- ============================================================
-- ÍNDICES DE PERFORMANCE
-- ============================================================

-- Empresas — consultas frequentes
CREATE INDEX idx_emp_estagio       ON empresas(estagio);
CREATE INDEX idx_emp_responsavel   ON empresas(responsavel_id);
CREATE INDEX idx_emp_seguradora    ON empresas(seguradora_alvo);
CREATE INDEX idx_emp_motor         ON empresas(motor);
CREATE INDEX idx_emp_prioridade    ON empresas(prioridade);
CREATE INDEX idx_emp_faixa         ON empresas(faixa);
CREATE INDEX idx_emp_ativo         ON empresas(ativo) WHERE ativo = TRUE AND excluido = FALSE;
CREATE INDEX idx_emp_sla           ON empresas(proxima_acao_em) WHERE proxima_acao_em IS NOT NULL;
CREATE INDEX idx_emp_nome_trgm     ON empresas USING gin(nome_devedor gin_trgm_ops);

-- Inscrições
CREATE INDEX idx_ins_cnpj_raiz     ON inscricoes(cnpj_raiz);
CREATE INDEX idx_ins_motor         ON inscricoes(motor);
CREATE INDEX idx_ins_tributo       ON inscricoes(tributo);
CREATE INDEX idx_ins_valor         ON inscricoes(valor_numerico DESC);
CREATE INDEX idx_ins_numero        ON inscricoes(numero_inscricao);

-- Interações
CREATE INDEX idx_int_cnpj_raiz     ON interacoes(cnpj_raiz);
CREATE INDEX idx_int_proxima_acao  ON interacoes(proxima_acao_em) WHERE proxima_acao_em IS NOT NULL;
CREATE INDEX idx_int_criado_em     ON interacoes(criado_em DESC);

-- Propostas
CREATE INDEX idx_prop_cnpj_raiz    ON propostas(cnpj_raiz);
CREATE INDEX idx_prop_status       ON propostas(status);

-- ============================================================
-- FUNCTIONS E TRIGGERS
-- ============================================================

-- Atualiza atualizado_em automaticamente
CREATE OR REPLACE FUNCTION fn_update_atualizado_em()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.atualizado_em = NOW();
  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_empresas_updated
  BEFORE UPDATE ON empresas
  FOR EACH ROW EXECUTE FUNCTION fn_update_atualizado_em();

CREATE TRIGGER trg_propostas_updated
  BEFORE UPDATE ON propostas
  FOR EACH ROW EXECUTE FUNCTION fn_update_atualizado_em();

CREATE TRIGGER trg_profiles_updated
  BEFORE UPDATE ON profiles
  FOR EACH ROW EXECUTE FUNCTION fn_update_atualizado_em();

-- Cria profile automaticamente após signup
CREATE OR REPLACE FUNCTION fn_handle_new_user()
RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  INSERT INTO profiles (id, nome, email, papel)
  VALUES (
    NEW.id,
    COALESCE(NEW.raw_user_meta_data->>'nome', split_part(NEW.email, '@', 1)),
    NEW.email,
    COALESCE((NEW.raw_user_meta_data->>'papel')::papel_usuario, 'operador')
  )
  ON CONFLICT (id) DO NOTHING;
  RETURN NEW;
END;
$$;

CREATE TRIGGER on_auth_user_created
  AFTER INSERT ON auth.users
  FOR EACH ROW EXECUTE FUNCTION fn_handle_new_user();

-- Registra mudança de estágio no histórico
CREATE OR REPLACE FUNCTION fn_registrar_mudanca_estagio()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.estagio IS DISTINCT FROM NEW.estagio THEN
    INSERT INTO historico_estagios (
      cnpj_raiz, estagio_anterior, estagio_novo, mudado_por, observacao
    ) VALUES (
      NEW.cnpj_raiz, OLD.estagio, NEW.estagio, NEW.atualizado_por,
      'Mudança automática via sistema'
    );
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_historico_estagio
  AFTER UPDATE ON empresas
  FOR EACH ROW EXECUTE FUNCTION fn_registrar_mudanca_estagio();

-- Recalcula agregados da empresa após insert/update de inscrições
CREATE OR REPLACE FUNCTION fn_recalcular_empresa()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
  v_cnpj_raiz CHAR(8);
  v_total     NUMERIC;
  v_maior     NUMERIC;
  v_qtd       INT;
  v_motor     motor_tipo;
BEGIN
  v_cnpj_raiz = COALESCE(NEW.cnpj_raiz, OLD.cnpj_raiz);

  SELECT
    COALESCE(SUM(valor_numerico), 0),
    COALESCE(MAX(valor_numerico), 0),
    COUNT(*)
  INTO v_total, v_maior, v_qtd
  FROM inscricoes
  WHERE cnpj_raiz = v_cnpj_raiz;

  -- Motor de maior urgência: A1 > B1 > B2 > A2
  SELECT motor INTO v_motor
  FROM inscricoes
  WHERE cnpj_raiz = v_cnpj_raiz AND motor IS NOT NULL
  ORDER BY
    CASE motor
      WHEN 'A1' THEN 1
      WHEN 'B1' THEN 2
      WHEN 'B2' THEN 3
      WHEN 'A2' THEN 4
    END
  LIMIT 1;

  UPDATE empresas SET
    valor_total_devida    = v_total,
    valor_maior_inscricao = v_maior,
    qtd_inscricoes        = v_qtd,
    motor                 = v_motor,
    atualizado_em         = NOW()
  WHERE cnpj_raiz = v_cnpj_raiz;

  RETURN COALESCE(NEW, OLD);
END;
$$;

CREATE TRIGGER trg_recalcular_empresa_ins
  AFTER INSERT OR UPDATE OR DELETE ON inscricoes
  FOR EACH ROW EXECUTE FUNCTION fn_recalcular_empresa();

-- ============================================================
-- VIEWS
-- ============================================================

-- KPIs do dashboard
CREATE OR REPLACE VIEW vw_dashboard_kpis AS
SELECT
  COUNT(*) FILTER (WHERE ativo AND NOT excluido)                           AS total_empresas,
  COUNT(*) FILTER (WHERE estagio = 'base_pgfn'       AND ativo AND NOT excluido) AS em_base,
  COUNT(*) FILTER (WHERE estagio = 'enriquecimento'  AND ativo AND NOT excluido) AS em_enriquecimento,
  COUNT(*) FILTER (WHERE estagio = 'abordagem'       AND ativo AND NOT excluido) AS em_abordagem,
  COUNT(*) FILTER (WHERE estagio = 'interesse_manifesto' AND ativo AND NOT excluido) AS em_interesse,
  COUNT(*) FILTER (WHERE estagio = 'analise_rapida'  AND ativo AND NOT excluido) AS em_analise,
  COUNT(*) FILTER (WHERE estagio IN ('proposta_enviada','submetido_sancor') AND ativo AND NOT excluido) AS em_proposta,
  COUNT(*) FILTER (WHERE estagio IN ('aprovado','fechado','receita_realizada') AND ativo AND NOT excluido) AS convertidos,
  SUM(valor_total_devida) FILTER (WHERE ativo AND NOT excluido)            AS total_divida_carteira,
  SUM(valor_total_devida) FILTER (WHERE estagio IN ('aprovado','fechado','receita_realizada')) AS divida_convertida,
  COUNT(*) FILTER (WHERE motor = 'A1' AND ativo AND NOT excluido)          AS motor_a1,
  COUNT(*) FILTER (WHERE motor = 'A2' AND ativo AND NOT excluido)          AS motor_a2,
  COUNT(*) FILTER (WHERE motor = 'B1' AND ativo AND NOT excluido)          AS motor_b1,
  COUNT(*) FILTER (WHERE motor = 'B2' AND ativo AND NOT excluido)          AS motor_b2,
  COUNT(*) FILTER (WHERE proxima_acao_em < NOW() AND ativo AND NOT excluido AND estagio != 'base_pgfn') AS followups_vencidos
FROM empresas;

-- Funil por estágio (para gráfico)
CREATE OR REPLACE VIEW vw_funil_estagio AS
SELECT
  estagio,
  COUNT(*)                AS qtd_empresas,
  SUM(valor_total_devida) AS valor_total,
  ROUND(AVG(score_vf))    AS score_medio
FROM empresas
WHERE ativo AND NOT excluido
GROUP BY estagio
ORDER BY
  CASE estagio
    WHEN 'base_pgfn'           THEN 1
    WHEN 'enriquecimento'      THEN 2
    WHEN 'abordagem'           THEN 3
    WHEN 'interesse_manifesto' THEN 4
    WHEN 'analise_rapida'      THEN 5
    WHEN 'proposta_enviada'    THEN 6
    WHEN 'submetido_sancor'    THEN 7
    WHEN 'aprovado'            THEN 8
    WHEN 'fechado'             THEN 9
    WHEN 'receita_realizada'   THEN 10
  END;

-- Empresas com SLA vencido (> 7 dias sem movimento)
CREATE OR REPLACE VIEW vw_sla_vencido AS
SELECT
  e.cnpj_raiz,
  e.nome_devedor,
  e.estagio,
  e.motor,
  e.prioridade,
  p.nome                                           AS responsavel_nome,
  e.atualizado_em,
  EXTRACT(DAY FROM NOW() - e.atualizado_em)::INT   AS dias_parado,
  e.proxima_acao_em,
  e.proxima_acao_descricao,
  e.valor_total_devida
FROM empresas e
LEFT JOIN profiles p ON e.responsavel_id = p.id
WHERE
  e.ativo AND NOT e.excluido
  AND e.estagio NOT IN ('base_pgfn', 'receita_realizada')
  AND NOW() - e.atualizado_em > INTERVAL '7 days'
ORDER BY dias_parado DESC;

-- ============================================================
-- ROW LEVEL SECURITY (RLS)
-- ============================================================

ALTER TABLE profiles            ENABLE ROW LEVEL SECURITY;
ALTER TABLE empresas            ENABLE ROW LEVEL SECURITY;
ALTER TABLE inscricoes          ENABLE ROW LEVEL SECURITY;
ALTER TABLE interacoes          ENABLE ROW LEVEL SECURITY;
ALTER TABLE consultas_seguradora ENABLE ROW LEVEL SECURITY;
ALTER TABLE propostas           ENABLE ROW LEVEL SECURITY;
ALTER TABLE historico_estagios  ENABLE ROW LEVEL SECURITY;

-- Profiles: usuário vê o próprio; admin vê todos
CREATE POLICY pol_profiles_select ON profiles FOR SELECT
  TO authenticated
  USING (auth.uid() = id OR EXISTS (
    SELECT 1 FROM profiles WHERE id = auth.uid() AND papel = 'admin'
  ));

CREATE POLICY pol_profiles_update ON profiles FOR UPDATE
  TO authenticated
  USING (auth.uid() = id);

-- Tabelas operacionais: qualquer autenticado CRUD
-- (em produção, refinar por papel se necessário)
CREATE POLICY pol_empresas_all ON empresas
  TO authenticated USING (TRUE) WITH CHECK (TRUE);

CREATE POLICY pol_inscricoes_all ON inscricoes
  TO authenticated USING (TRUE) WITH CHECK (TRUE);

CREATE POLICY pol_interacoes_all ON interacoes
  TO authenticated USING (TRUE) WITH CHECK (TRUE);

CREATE POLICY pol_consultas_all ON consultas_seguradora
  TO authenticated USING (TRUE) WITH CHECK (TRUE);

CREATE POLICY pol_propostas_all ON propostas
  TO authenticated USING (TRUE) WITH CHECK (TRUE);

CREATE POLICY pol_historico_all ON historico_estagios
  TO authenticated USING (TRUE) WITH CHECK (TRUE);

-- ============================================================
-- DADOS INICIAIS (seed)
-- ============================================================

-- Usuários Rodrigo e Ana são criados pelo Supabase Dashboard → Authentication
-- Após criar, execute:
-- UPDATE profiles SET papel = 'admin' WHERE email = 'rodrigo@vazquezfonseca.com.br';
-- UPDATE profiles SET papel = 'operador' WHERE email = 'ana@vazquezfonseca.com.br';
