-- ============================================================
-- 35 · PAUTA — a linha de trabalho diária
-- Aplicada em produção em 2026-08-19 (migration: pauta_linha_de_trabalho).
-- Regra de negócio (Rodrigo, 19/08): mín. 5, alvo 10 itens/dia POR SÓCIO.
-- A pauta é o oposto do acervo: só entra o que o playbook selecionou.
-- Aditiva e idempotente. Rollback comentado no fim.
-- ============================================================

CREATE TABLE IF NOT EXISTS pauta (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  data date NOT NULL,
  cnpj_raiz char(8) NOT NULL,
  responsavel text NOT NULL CHECK (responsavel IN ('RODRIGO','ANA')),
  padrao text NOT NULL,               -- P0..P5 do playbook (ver sql/pauta_selecao_playbook.sql)
  numero_processo text,
  motivo text,                        -- gatilho em uma frase ("★ " prefixa destaque)
  alerta text,                        -- ressalva de conferência, se houver
  concluido boolean NOT NULL DEFAULT false,
  criado_em timestamptz NOT NULL DEFAULT now(),
  UNIQUE (data, cnpj_raiz)
);

CREATE INDEX IF NOT EXISTS idx_pauta_data ON pauta (data, responsavel);

ALTER TABLE pauta ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS pauta_all ON pauta;
CREATE POLICY pauta_all ON pauta FOR ALL TO authenticated USING (true) WITH CHECK (true);

-- ROLLBACK:
-- DROP TABLE IF EXISTS pauta;
