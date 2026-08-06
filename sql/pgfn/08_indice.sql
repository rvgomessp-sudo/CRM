-- Índice para acelerar as queries de CNPJ limpo
CREATE INDEX IF NOT EXISTS idx_cnpj_limpo
  ON arquivo_lai_sida ((REGEXP_REPLACE("CPF_CNPJ", '[^0-9]', '', 'g')));

CREATE INDEX IF NOT EXISTS idx_tipo_devedor
  ON arquivo_lai_sida ("TIPO_DEVEDOR");

ANALYZE arquivo_lai_sida;
