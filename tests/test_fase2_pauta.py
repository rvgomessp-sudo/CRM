# ============================================================
# TESTES · Fase 2 — Pauta 20/08 como fixture auditável
# Roda contra o banco com: PG_CONN=postgresql://... python tests/test_fase2_pauta.py
# Pré-requisito: migration 36 aplicada + fixture pauta_20_08 carregada.
# Sem framework externo — asserts puros, saída legível, exit code != 0 em falha.
# ============================================================
import os, sys, json
import psycopg2

DSN = os.environ.get("PG_CONN")
if not DSN:
    print("PULADO: defina PG_CONN para rodar (não armazenar a credencial em arquivo).")
    sys.exit(0)

conn = psycopg2.connect(DSN)
cur = conn.cursor()
FALHAS = []

def check(nome, sql, prova):
    cur.execute(sql)
    ok = bool(cur.fetchone()[0])
    print(f"{'PASS' if ok else 'FAIL'}  {nome}")
    if not ok:
        FALHAS.append((nome, prova))

F = "fonte IN ('PAUTA_20_08+DJEN_COMUNICA','AUDITORIA_FASE_2')"

# --- Importação como candidatos (critério: 20 registros, nenhum promovido) ---
check("20 alertas da pauta importados",
      f"SELECT count(*)=20 FROM alertas WHERE fonte='PAUTA_20_08+DJEN_COMUNICA'",
      "esperado exatamente 20 candidatos vindos da pauta")
check("nenhum alerta nasce VALIDADO ou promovido",
      f"SELECT count(*)=0 FROM alertas WHERE {F} AND estado NOT IN ('CANDIDATO','AGUARDANDO_VALIDACAO')",
      "estado inicial deve ser CANDIDATO")
check("divisão 10 Rodrigo / 10 Ana preservada",
      "SELECT (SELECT count(*) FROM alertas WHERE fonte='PAUTA_20_08+DJEN_COMUNICA' AND responsavel='RODRIGO')=10 "
      "AND (SELECT count(*) FROM alertas WHERE fonte='PAUTA_20_08+DJEN_COMUNICA' AND responsavel='ANA')=10",
      "divisão operacional da pauta")

# --- Casos nominais exigidos pelos critérios de aceite ---
check("Della Coletta: papel processual PENDENTE",
      "SELECT count(*)=1 FROM alertas WHERE cnpj_raiz='44691236' AND papel_processual='PENDENTE' "
      "AND pendencias ? 'PAPEL_PROCESSUAL_PENDENTE'",
      "publicação nomeia outro réu principal")
check("Maxlog: CREDORA com evidência em CONFLITO",
      "SELECT count(*)=1 FROM alertas WHERE cnpj_raiz='10447922' AND papel_processual='CREDORA' "
      "AND evidencia_condicao='CONFLITO'",
      "empresa exequente na publicação usada")
check("AC Coelho: CREDORA com evidência em CONFLITO",
      "SELECT count(*)=1 FROM alertas WHERE cnpj_raiz='37083474' AND papel_processual='CREDORA' "
      "AND evidencia_condicao='CONFLITO'",
      "empresa exequente na publicação usada")
check("Aguapeí: grupo econômico não confirmado",
      "SELECT count(*)=1 FROM alertas WHERE cnpj_raiz='35203047' AND pendencias ? 'GRUPO_ECONOMICO_NAO_CONFIRMADO'",
      "embargante é a holding; identidade não confirmada")
check("N M Engenharia: candidato crítico com teor corroborado",
      "SELECT count(*)=1 FROM alertas WHERE cnpj_raiz='51594950' AND gravidade='CRITICO' "
      "AND evidencia_condicao='CORROBORADO' AND papel_processual='DEVEDORA'",
      "embargos extintos por ausência de garantia — teor DJEN presente")
check("Pandurata: fonte original ausente detectada",
      "SELECT count(*)=1 FROM alertas WHERE cnpj_raiz='70940994' AND pendencias ? 'FONTE_ORIGINAL_AUSENTE'",
      "link genérico do DJE-TJSP, sem documento específico")

# --- Regras transversais ---
check("reteste de recorrência PGFN pendente em TODOS",
      f"SELECT count(*)=0 FROM alertas WHERE fonte='PAUTA_20_08+DJEN_COMUNICA' "
      "AND NOT (pendencias ? 'RETESTE_RECORRENCIA_PGFN')",
      "detecção 1 da ordem")
check("dívida PGFN nunca copiada para valor_processo",
      f"SELECT count(*)=0 FROM alertas a JOIN vw_fila_oportunidades f USING (cnpj_raiz) "
      f"WHERE a.fonte='PAUTA_20_08+DJEN_COMUNICA' AND a.valor_processo = f.valor_total_devida "
      "AND a.valor_processo IS NOT NULL",
      "detecção 7: dívida ≠ valor processual")
check("eventos trabalhistas sinalizados como não-fiscais",
      f"SELECT count(*)=0 FROM alertas WHERE fonte='PAUTA_20_08+DJEN_COMUNICA' AND ramo='TRABALHISTA' "
      "AND NOT (pendencias ? 'EVENTO_TRABALHISTA_NAO_E_DIVIDA_FISCAL')",
      "detecção 8 da ordem")
check("score legado registrado como histórico (coluna própria)",
      f"SELECT count(*)>0 FROM alertas WHERE fonte='PAUTA_20_08+DJEN_COMUNICA' AND score_legado IS NOT NULL",
      "score do PDF não é autoridade; vive em score_legado")
check("inconsistência 3×4 avisos da pauta detectada",
      "SELECT count(*)=1 FROM alertas WHERE fonte='AUDITORIA_FASE_2' "
      "AND titulo LIKE 'Inconsistência documental%'",
      "detecção 6: o documento declara 3 avisos; registros = 4")
check("12 críticos (zona sufoco da pauta)",
      "SELECT count(*)=12 FROM alertas WHERE fonte='PAUTA_20_08+DJEN_COMUNICA' AND gravidade='CRITICO'",
      "PDF classifica 12 como zona sufoco")

# --- Guarda de promoção (validação humana obrigatória) ---
cur.execute("SELECT id FROM alertas WHERE fonte='PAUTA_20_08+DJEN_COMUNICA' AND estado='CANDIDATO' LIMIT 1")
row = cur.fetchone()
if row:
    try:
        cur.execute("SELECT fn_promover_alerta(%s, NULL)", (row[0],))
        conn.rollback()
        print("FAIL  promoção sem validação foi aceita (não deveria)")
        FALHAS.append(("guarda de promoção", "fn_promover_alerta aceitou CANDIDATO"))
    except psycopg2.Error:
        conn.rollback()
        print("PASS  promoção de CANDIDATO é bloqueada (validação humana obrigatória)")

print()
if FALHAS:
    print(f"{len(FALHAS)} FALHA(S):")
    for nome, prova in FALHAS:
        print(f"  - {nome}: {prova}")
    sys.exit(1)
print("TODOS OS TESTES DA FASE 2 PASSARAM.")
cur.close(); conn.close()
