-- ============================================================
-- 39 · Contato dos decisores (telefone/e-mail para prospecção)
-- A tela /decisores alimenta estes campos. Aditiva, idempotente.
-- ============================================================
ALTER TABLE decisores ADD COLUMN IF NOT EXISTS telefone text;
ALTER TABLE decisores ADD COLUMN IF NOT EXISTS email    text;
ALTER TABLE decisores ADD COLUMN IF NOT EXISTS linkedin text;
ALTER TABLE decisores ADD COLUMN IF NOT EXISTS observacao text;
-- ROLLBACK:
-- ALTER TABLE decisores DROP COLUMN IF EXISTS telefone, DROP COLUMN IF EXISTS email,
--   DROP COLUMN IF EXISTS linkedin, DROP COLUMN IF EXISTS observacao;
