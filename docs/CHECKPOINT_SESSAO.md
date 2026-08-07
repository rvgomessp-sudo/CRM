# 🏁 CHECKPOINT — Sessão CRM V3 / Base v2 / Solver v2

> Ponto de retomada. Última atualização: fim da sessão (madrugada).
> Grupo Vazquez & Fonseca — Rodrigo Vazquez.

---

## ✅ O QUE ESTÁ PRONTO E NO AR

### CRM V3 operacional
- URL produção: **https://crm-x874.vercel.app**
- Stack: Next.js 14 + Supabase (projeto `pnmasrynyiaqhxkcqygj`) + Vercel (`crm-x874`)
- Repo GitHub: `rvgomessp-sudo/CRM` (público)
- Login: rodrigo.vazquez@vazquezefonseca.com.br (admin) + ana.fonseca@... (operador)
- Telas funcionando: Dashboard, Pipeline (Kanban), Base PGFN, ficha da empresa
  (Overview/Inscrições/Interações/Proposta), VF Solver v1, Importar
- Bugs corrigidos nesta sessão: join profiles ambíguo (RLS bloqueava Pipeline e
  ficha), filtros base-pgfn, UFs incompletas — todos resolvidos e deployados

### Carga v1 atual no banco
- 42 empresas + 42 inscrições (motor A1) — SERÃO SUBSTITUÍDAS pela v2

### Documentação versionada no repo (docs/)
- REGRAS_ORIGINACAO_PGFN.md — 11 checkpoints da regra de corte
- SPEC_VF_SOLVER_V2.md — especificação completa do Solver novo
- sql/pgfn/ — scripts de índice e materialização

---

## 🎯 BASE V2 — PRONTA NO POSTGRES (falta carregar)

Tabela `crm_carga_v2` materializada no Postgres local (banco `postgres`).

**Números finais:**
- **2.823 empresas** | 113.178 inscrições | **R$49,3 bilhões** de pipeline
- Média ~40 inscrições por CNPJ (passivo fragmentado)

**Régua aplicada (confirmada):**
- Soma das inscrições por CNPJ >= R$3MM (o "joelho" da curva de Pareto)
- Marinheiro: TODAS as inscrições >=2023 (qualquer dívida pré-2023 descarta o CNPJ)
- PRINCIPAL, PJ (14 dígitos)
- Exclui: Simples Nacional, RJ/Falência/Massa, públicos (município/prefeitura/
  estado/câmara), sem fins lucrativos (associação/instituto/fundação/sindicato/
  sociedade educação/assistência)
- Valor sem teto (Pareto: 96,9% da base bruta < R$500k foi descartada)

**Distribuição por motor (motor principal por CNPJ):**
- A2 (não ajuizado, sem garantia) — MAIOR grupo, carro-chefe da prevenção
- A1 (ajuizado, sem garantia) — urgência BACENJUD
- B2 (seguro garantia) — substituição antecipada
- B1 (penhora), B3 (carta fiança), B4 (depósito), B5 (NJP)

---

## 🧭 DECISÃO PENDENTE — RETOMAR AQUI AMANHÃ

**Rodrigo parou (acertadamente) para mexer na ESTRUTURA do CRM antes de carregar.**

### As 2 perguntas abertas:
1. **Até onde reformar antes de carregar?**
   - Camada 1: mínimo p/ destravar (motores B3/B4/B5 no enum do banco +
     ficha multi-inscrição + Pipeline com coluna de descarte) → Ana opera já
   - Camada 1+2: acima + VF Solver v2 completo
   - Reforma completa antes
   - Desenhar tudo primeiro

2. **Urgência de pôr a Ana operando pesa?** (contexto de caixa apertado)

### Camadas mapeadas (do backlog do Rodrigo):
- **Camada 1 (destrava operação):** motores B3-B5 no schema; ficha exibir bem
  40+ inscrições; Pipeline coluna descarte; carregar v2
- **Camada 2 (motor econômico):** VF Solver v2 (honorário por valor destravado,
  módulo substituição, motores diferenciados) — ver SPEC_VF_SOLVER_V2.md
- **Camada 3 (produto SaaS):** white-label (paleta/logo/nome), ambientes
  separados, assinatura com fee, segregação de visão Ana/Rodrigo

---

## 🐛 BACKLOG DE AJUSTES (do teste do Rodrigo)

**VF Solver:**
- Formatar valor em R$ (hoje sem formatação)
- Teto 80-200K fixo → SUBSTITUIR por % do valor destravado (spec pronta)
- CNPJ raiz deve puxar o tomador e dados (auto-preenche)
- "Salva mas não gera" → falta output PDF da proposta

**Telas:**
- Inscrições: desalinhamento de colunas (valor sob "Garantia", faltam Tributo/
  Dias/Garantia) — reordenar conforme ordem definida
- Pipeline: falta coluna/estágio de DESCARTE (empresa reprovada não tem p/ onde ir)
- Importar: obsoleta (base vem do SQL agora) — remover ou repropositar
- Overview: definir fonte dos dados (enriquecimento futuro)
- Interações: reformular formulário, automatizar, criar inteligência
- Proposta: reformular (ligar ao Solver v2)
- Dashboard: modernizar gráficos/KPIs

**Estrutura:**
- Segregação de bases Ana/Rodrigo não existe mais (recriar visão)
- Enriquecimento futuro: natureza jurídica da Receita (CP-11) para filtrar
  estatais/entidades com precisão (SPTrans, CODEBA passaram na v1)

---

## 💡 CONCEITO DO VF SOLVER V2 (consolidado — não perder)

Modelo V&F vs corretor tradicional:
- Corretor agrava taxa (0,20%→0,50%) p/ inflar prêmio e comissão (limitada 25%)
- V&F repassa taxa limpa + honorário explícito pelo trabalho = transparência

Honorário = MAX(%_motor × Valor_Destravado, PISO R$30k)
- Valor destravado varia por motor (IS / ativo / restituição / limite / depósito futuro)
- Substituição antecipada: restituição prêmio não decorrido (tabela prazo curto,
  ajustável) + economia na taxa. Apólice recente = alvo mais quente (recupera quase tudo)

Decisões ainda abertas do Solver:
- Ponto default do honorário na faixa (margem vs volume — ligado a elasticidade)
- Calibrar % por motor (faixas de partida na spec)
- Estudo de taxa próprio para automatizar comparativo
