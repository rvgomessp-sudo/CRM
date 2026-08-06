# 📐 Especificação — VF Solver v2

> Motor de precificação de honorários do Grupo Vazquez & Fonseca.
> Reformulação completa do conceito: de "calculadora de comissão" para
> "otimizador de honorário ancorado no valor econômico destravado".

---

## 🎯 Conceito Central

**O tomador não compra a apólice — compra um resultado jurídico-econômico:**
- Suspender execução fiscal / exigibilidade (evita BACENJUD, bloqueio de caixa)
- Substituir penhora (libera ativo imobilizado)
- Substituir garantia onerosa (libera limite bancário / caixa)

**Posicionamento V&F (vs. corretor tradicional):**
- Corretor tradicional: agrava a taxa da Cia (0,20% → 0,50%) para inflar o
  prêmio e, com isso, a base sobre a qual incide sua comissão (limitada a 25%).
  Opacidade. Cliente paga sobrepreço escondido na taxa.
- V&F: repassa a taxa da Cia **limpa, sem agravo**. Recebe a comissão de 25%
  sobre o prêmio limpo + honorário **explícito** pelo trabalho. Transparência.

---

## 🧮 Fórmula do Honorário

```
Honorário = MAX( %_motor × Valor_Destravado , PISO_VIABILIDADE )
```

- **PISO_VIABILIDADE = R$ 30.000** (fixo, qualquer motor, qualquer tamanho)
- **Valor_Destravado** calculado por motor (ver tabela)
- **%_motor** = faixa calibrável por tipo de operação

### Valor Destravado por Motor

| Motor | Situação | Valor Destravado = | % sugerido (calibrar) |
|-------|----------|--------------------|-----------------------|
| A1/A2 | Execução, sem garantia | IS (dívida garantida) | 0,50% – 1,50% |
| B1 | Penhora | Valor do ativo liberado (input) | 0,40% – 1,20% |
| B2 | Seguro Garantia (substituição) | Restituição + economia na taxa | 1,00% – 3,00% |
| B3 | Carta Fiança (migração) | Capital/limite bancário liberado | 0,50% – 1,50% |

> Percentuais são pontos de partida, NÃO benchmarks de mercado. Rodrigo calibra.

---

## 🔄 Módulo Substituição Antecipada (Motor B2)

Diferencial competitivo: tomador com apólice recente e cara não precisa esperar
o aniversário. Cancela e migra, recuperando prêmio não decorrido.

**Inputs adicionais:**
- Prêmio pago na apólice atual
- Data de emissão / % da vigência decorrida
- Taxa comparativa (manual — até ter estudo de taxa próprio)

**Cálculo:**
```
Restituição = Prêmio_pago × %_prazo_curto(tempo_decorrido)
Economia    = Prêmio_pago − Prêmio_novo_VF
Valor_Troca = Restituição + Economia
```

**Restituição — tabela de prazo curto SUSEP (regressiva):**
- ATENÇÃO: tabela clássica SUSEP é para apólices ≤12 meses. Seguro garantia
  multi-anual (60 meses) usa pro rata die ou tabela adaptada. Implementar como
  parâmetro AJUSTÁVEL, calibrar com dados reais. Não afirmar % como exatos.
- Curva regressiva: penaliza cancelamento antecipado.
  Ex. (estimativa): 10% decorrido → ~90% restituição; 50% → ~40%; fim → 0%.

**Exemplo (IS 20MM, apólice atual 0,50%, nova 0,20%):**
| Momento apólice | Restituição | + Economia | = Valor ao cliente |
|-----------------|-------------|------------|--------------------|
| Recente (10%) | R$ 450K | R$ 300K | **R$ 750K** |
| Metade (50%) | R$ 200K | R$ 300K | R$ 500K |
| Vai renovar | R$ 0 | R$ 300K | R$ 300K |

---

## 📊 Inputs do Solver (todos)

| Input | Origem | Editável |
|-------|--------|----------|
| IS (importância segurada) | Contrato | sim |
| Taxa a.a. da Cia | Seguradora (limpa) | sim (travável) |
| Vigência (anos) | Contrato | sim |
| % Comissão | Cia (default 25%) | sim |
| Motor | Classificação da inscrição | auto/manual |
| Valor destravado | Depende do motor | sim (penhora/fiança) |
| Prêmio pago (subst.) | Apólice atual | sim |
| % vigência decorrida | Apólice atual | sim |
| Taxa comparativa | Manual | sim |

## 📈 Outputs do Solver

- Prêmio total (IS × taxa × vigência)
- Comissão V&F limpa (prêmio × %com)
- **Honorário ótimo** (fórmula acima)
- Receita V&F total (comissão + honorário)
- Custo ao cliente (prêmio + honorário)
- **Comparativo vs. corretor tradicional** (economia entregue)
- Valor destravado / valor da troca (substituição)

---

## 🐛 Bugs do Solver v1 a corrigir

- [ ] Valor da garantia sem formatação R$ (dificulta leitura)
- [ ] Teto R$80-200K fixo descalibra micro e macro → SUBSTITUIR por % do valor destravado
- [ ] CNPJ raiz não puxa o tomador nem dados → vincular à empresa (auto-preenche)
- [ ] "Salva mas não gera" → falta output da proposta (PDF)
- [ ] Regra econômica precisa refletir novo modelo (honorário ancorado, não janela fixa)

---

## ⏳ Pendências (decisões do Rodrigo)

- [ ] Calibrar os % por motor (faixas acima são partida)
- [ ] Estudo de taxa próprio (para automatizar taxa comparativa da base)
- [ ] Ponto default do honorário na faixa (margem vs. volume — ligado à elasticidade)

---

## 🔧 ATUALIZAÇÃO — Motores expandidos (base v2)

Após mapeamento dos tipos de garantia na base (SITUACAO_INSCRICAO):

| Motor | Situação (SITUACAO_INSCRICAO) | Vetor | Valor Destravado |
|-------|-------------------------------|-------|------------------|
| A1 | ATIVA AJUIZADA (sem garantia) | Urgência BACENJUD | IS |
| A2 | ATIVA NAO AJUIZAVEL (sem gar.) | Prevenção | IS |
| B1 | ...GARANTIA - PENHORA | Substituição libera ativo | Valor do ativo |
| B2 | ...GARANTIA - SEGURO GARANTIA | Substituição antecipada | Restituição + economia |
| B3 | ...GARANTIA - CARTA FIANCA | Migração taxa menor | Limite bancário liberado |
| B4 | ...GARANTIA - DEPOSITO | **Impedir NOVOS depósitos** | Depósitos futuros evitados |
| B5 | ...GARANTIA - NJP | Estruturação premium | A definir |

### ⚠️ CORREÇÃO CRÍTICA — Motor B4 (Depósito)
A PGFN **NÃO aceita** substituir depósito judicial já feito por seguro garantia.
O dinheiro depositado permanece. Portanto o B4 NÃO é "liberar caixa preso".
O vetor correto é **PREVENÇÃO**: impedir que a empresa continue imobilizando
caixa em NOVOS depósitos. Argumento: "em vez de depositar mais no tribunal,
garanta por seguro — preserve seu caixa daqui pra frente."
Valor destravado = valor estimado de novos depósitos evitados (não o já depositado).

### Volumes na base (PJ, principal, situação):
- Seguro Garantia: 20.659 | Depósito: 16.142 | Penhora: ~5.478 (qualif.)
- Carta Fiança: 2.709 | NJP: 282
