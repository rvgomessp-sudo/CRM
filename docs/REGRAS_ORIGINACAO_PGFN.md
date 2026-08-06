# 📋 Regras de Originação — Base PGFN → CRM V3 (V&F)

> Documento vivo. Consolida as regras de corte, exclusão e classificação
> aplicadas na seleção de alvos de Seguro Garantia Tributário a partir da
> base PGFN (`arquivo_lai_sida`, 21.554.957 inscrições).
>
> **Última atualização:** carga inicial NR1
> **Responsável técnico:** Rodrigo Vazquez (Corecon-SP 32.884)

---

## 🎯 Tese de Originação

O alvo NÃO é o grande devedor antigo (tem estrutura jurídica, sabe usar
penhora/embargos, já tem fornecedor de garantia). O alvo é o **marinheiro
de primeira viagem**: PJ recém-chegada ao contencioso fiscal, dívida fresca,
sem estrutura de defesa, que **precisa de garantia agora**.

---

## ✅ CHECKPOINTS — Regras Consolidadas

### CP-01 · Fonte e chave
- [x] Fonte única: PostgreSQL local, tabela `arquivo_lai_sida` (NUNCA mais CSV)
- [x] Chave natural do card: `NUMERO_INSCRICAO` (1 inscrição = 1 card)
- [x] CNPJ raiz = 8 primeiros dígitos de `CPF_CNPJ` limpo (só números)

### CP-02 · Princípio de dados
- [x] **PRESERVAR TODAS AS COLUNAS** na materialização
- [x] Filtrar por CRITÉRIO (WHERE), nunca eliminando campos (SELECT)
- [x] Coluna a mais é barata; coluna perdida custa retrabalho

### CP-03 · Marinheiro de Primeira Viagem
- [x] CNPJ com **inscrição ÚNICA** na base inteira (aparece 1x como PRINCIPAL)
- [x] Se tem 2+ inscrições (qualquer data) → reincidente → EXCLUÍDO
- [x] Data da única inscrição ≥ 01/01/2023

### CP-04 · Devedor
- [x] Somente `TIPO_DEVEDOR = 'PRINCIPAL'`
- [x] Somente `TIPO_PESSOA = 'Pessoa jurídica'`
- [x] CPF_CNPJ com 14 dígitos (CNPJ, não CPF)
- [ ] FASE 2: incluir CORRESPONSÁVEL/SOLIDÁRIO (1 inscrição ↔ N devedores)

### CP-05 · Motor
- [x] A1 = ajuizado (`INDICADOR_AJUIZADO = SIM`) sem garantia
- [x] B1 = penhora (raro nesta base: ~64 casos — penhora está no TRF3, não PGFN)
- [x] Prioridade empresa: A1 > B1 > B2 > A2

### CP-06 · Exclusões por situação
- [x] `TIPO_SITUACAO_INSCRICAO <> 'Garantia'`
- [x] `SITUACAO_INSCRICAO NOT LIKE '%GARANTIA%'` (ex: garantia por PL, Lei 14.689/2023)
- [x] `SITUACAO_INSCRICAO NOT LIKE '%SISPAR%'` (já em parcelamento/transação)

### CP-07 · Exclusões por natureza do devedor (via NOME_DEVEDOR)
- [x] Recuperação Judicial: `NOT LIKE '%RECUPERACAO%'`
- [x] Falência: `NOT LIKE '%FALIDO%'` e `NOT LIKE '%MASSA FALIDA%'`
- [x] Entes públicos: MUNICIPIO, PREFEITURA, ESTADO DE, CAMARA MUNICIPAL
- [x] Sem fins lucrativos: ASSOCIACAO, FUNDACAO, SINDICATO, INSTITUTO, SOCIEDADE...ASSISTENCIA
- [ ] PENDENTE: estatais sem marcador no nome (ex: SP Transporte) — refinar por CNAE/natureza jurídica na fase 2

### CP-08 · Exclusões por tributo
- [x] `RECEITA_PRINCIPAL NOT ILIKE '%Simples Nacional%'`
      (empresa do Simples não acessa mercado de garantia tributária)

### CP-09 · Piso de valor
- [x] `VALOR_CONSOLIDADO >= R$ 1.000.000` (piso vigente)
- Alternativas testadas: R$3MM (mais qualidade), R$5MM, R$15MM

### CP-10 · Deduplicação por solidário (ALERTA CRÍTICO)
- [x] Base tem 6,97 milhões de linhas duplicadas por corresponsável/solidário
- [x] Soma bruta infla 44% (R$758bi vs R$427bi real) — NUNCA somar sem filtrar PRINCIPAL
- [x] Filtro `TIPO_DEVEDOR = 'PRINCIPAL'` resolve

---

## 📊 Perfil Tributário dos Alvos (piso R$1MM)

Tributos que levam os marinheiros ao judiciário:
- **IPI** — ticket médio R$29,9MM (indústria)
- **Multa Isolada** — R$15,3MM (autuação fiscal, dor aguda)
- **IRRF, Imposto Importação, Contribuição Previdenciária** — R$12-59MM

Perfil do cliente ideal: **indústria/importadora de porte médio,
autuada recentemente, dívida de IPI ou multa isolada.**

---

## 🔮 FASE 2 (planejado, não implementado)

- [ ] Modelo N:N: 1 inscrição ↔ vários CNPJ/CPF solidários
- [ ] Solidário solvente de empresa em RJ = lead premium
- [ ] Schema do CRM evolui: tabela `devedores` separada de `inscricoes`
- [ ] Cruzamento com TRF3/DataJud para status processual (penhora, fase)
- [ ] Enriquecimento: PL, faturamento, CNAE, decisor (OSINT)

---

## 🗂️ Artefatos SQL (ordem de execução)

| # | Arquivo | Função |
|---|---------|--------|
| 08 | `08_indice.sql` | Índices de performance (rodar 1x) |
| 12 | `12_rematerializar_completo.sql` | Cria `crm_alvos_nr1` (todas colunas + exclusões) |
| — | `gerar_inserts` | Exporta INSERTs empresas+inscricoes p/ Supabase |


---

## CP-11 · Filtro por Natureza Jurídica (FASE ENRIQUECIMENTO)

> Substitui os filtros frágeis por nome (CP-07) por classificação precisa
> da Receita Federal, via campo `natureza_juridica` do cadastro CNPJ.

- [ ] Enriquecer base com `natureza_juridica` (código 4 dígitos + descrição) da Receita
- [ ] Filtrar por CÓDIGO, não por texto do nome:

| Código | Natureza | Decisão |
|--------|----------|---------|
| 2062 | Soc. Empresária Limitada | ✅ alvo |
| 2054 | S.A. Fechada | ✅ alvo |
| 2046 | S.A. Aberta | ⚠️ avaliar porte |
| 2135 | Empresário Individual | 🔶 régua a definir |
| 1244 | Adm. Pública / Autarquia | ❌ excluir (SPTrans, CODEBA) |
| 2038 | Economia Mista | ❌ excluir (estatal) |
| 3999 | Associação / Fundação | ❌ excluir (sem fins lucrativos) |

- [ ] Aposentar filtros por nome (MUNICIPIO, ASSOCIACAO...) após ter natureza_juridica
- [ ] Casos hoje resolvidos na mão (SPTrans, CODEBA, Federação, empresário individual) passam a ser automáticos

### Casos pendentes de julgamento (carga inicial, resolver no enriquecimento)
- SÃO PAULO TRANSPORTE S.A. — provável Adm. Pública/Economia Mista
- COMPANHIA DAS DOCAS DA BAHIA (CODEBA) — provável Economia Mista
- FEDERAÇÃO BRASILEIRA DE CONVENTION — provável Associação
- SAMARA VANESSA DE OLIVEIRA, ARYAN SCHUT FLORES — provável Empresário Individual
