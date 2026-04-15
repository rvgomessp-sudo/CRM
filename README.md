# V&F CRM v3.0 — Seguro Garantia Tributário PGFN

CRM operacional para prospecção e conversão de operações PGFN (garantia judicial tributária).  
Stack: Next.js 14 (App Router) + Supabase + Vercel

---

## Setup — passo a passo

### 1. Supabase

1. Acesse [supabase.com](https://supabase.com) → novo projeto
2. Abra **SQL Editor** e execute o arquivo `schema_supabase_crm_v3.sql` (gerado separadamente)
3. Vá em **Authentication → Users** e crie os usuários:
   - `rodrigo@vf.com.br` (senha forte)
   - `ana@vf.com.br` (senha forte)
4. Copie o **Project URL** e a **anon public key** de Settings → API

### 2. Usuários na tabela

Após criar os usuários no Auth, rode no SQL Editor:

```sql
INSERT INTO usuarios (auth_user_id, nome, email, papel) VALUES
('<uuid-rodrigo>', 'Rodrigo Vazquez', 'rodrigo@vf.com.br', 'admin'),
('<uuid-ana>', 'Ana', 'ana@vf.com.br', 'operador');
```

O UUID está em Authentication → Users → coluna "UID".

### 3. Projeto local

```bash
git clone https://github.com/rvgomessp-sudo/CRM.git
cd CRM
npm install

# Copiar e preencher variáveis
cp .env.example .env.local
# editar .env.local com NEXT_PUBLIC_SUPABASE_URL e NEXT_PUBLIC_SUPABASE_ANON_KEY

npm run dev
```

Acesse: http://localhost:3000

### 4. Deploy na Vercel

```bash
# Instalar Vercel CLI (se necessário)
npm i -g vercel

vercel --prod
```

Ou via interface:
1. [vercel.com](https://vercel.com) → Import Git Repository → `rvgomessp-sudo/CRM`
2. Framework: **Next.js** (detectado automaticamente)
3. Environment Variables:
   - `NEXT_PUBLIC_SUPABASE_URL`
   - `NEXT_PUBLIC_SUPABASE_ANON_KEY`
4. Deploy

---

## Importar base F2

1. Acesse `/importar` no CRM
2. Arraste o arquivo `01_f2_inscricoes_individuais.csv`
3. O importador faz dedup automático por CNPJ_RAIZ e NUMERO_INSCRICAO
4. O pipeline existente NÃO é sobrescrito em reimportações

---

## Estrutura de arquivos

```
app/
  (painel)/
    dashboard/        ← KPIs, funil, follow-ups vencidos
    pipeline/         ← Kanban 10 etapas
    base/             ← Tabela base PGFN com filtros
    empresa/[cnpj]/   ← Ficha completa com inscrições individuais
    importar/         ← Upload CSV F1/F2
    solver/           ← Precificação ótima
    configuracoes/    ← Supabase, usuários, backup
  login/
components/
  Sidebar.tsx
lib/
  supabase/client.ts  ← browser
  supabase/server.ts  ← SSR
  types.ts            ← todos os tipos TypeScript
  utils.ts            ← formatBRL, CNPJ, cálculos
  importador.ts       ← parser CSV + upsert Supabase
middleware.ts         ← proteção de rotas
```

---

## Regras de negócio críticas

- **CNPJ_RAIZ**: 8 primeiros dígitos numéricos, zero-padded. NUNCA contar pontos/barras.
- **Dedup**: por `NUMERO_INSCRICAO` nas inscrições. Empresa atualiza dados mas preserva estágio do pipeline.
- **Exclusão automática**: Massa Falida, Recuperação Judicial, Falido, Simples, MEI.
- **Regra econômica**: `comissão + honorários > prêmio líquido` — obrigatória em toda proposta.
- **SLA**: análise rápida 1 dia útil, proposta 48h, alerta >7 dias parado.
- **Ordem da esteira**: PGFN → Receita/CNPJ → Sancor → decisor. Enriquecimento de decisores só após validação cadastral + consulta Sancor.

---

## Tiers de seguradora (regra de alocação automática)

| Seguradora | Dívida | PL mínimo | Receita mínima |
|---|---|---|---|
| Sancor | ≤ R$ 20M | R$ 20M | — |
| Berkley | ≤ R$ 30M | — | R$ 100M |
| Zurich/Swiss/Chubb | > R$ 30M | R$ 100M | R$ 300M |

---

## Proteção de PI (casos fora da automaticidade)

Antes da defesa técnica com Rodrigo: NDA + termo de cooperação + instrumento de PI + documentos financeiros + ficha judicial.
