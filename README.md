# CRM V3 — Vazquez & Fonseca

Pipeline PGFN · Seguro Garantia Tributário · Stack: Next.js 14 + Supabase + Vercel

---

## Stack

| Camada | Tecnologia |
|--------|-----------|
| Frontend | Next.js 14 App Router + Tailwind CSS |
| Backend/DB | Supabase (PostgreSQL 15) |
| Auth | Supabase Auth (email/password) |
| Deploy | Vercel |
| CSV | PapaParse (client-side, UTF-8 BOM) |

---

## Deploy — Passo a Passo

### 1. Supabase — criar projeto

1. Acesse https://supabase.com/dashboard
2. Crie um projeto em **São Paulo (sa-east-1)**
3. Copie a **URL** e a **anon key** do projeto
4. Vá em **SQL Editor** e execute o arquivo `crm_v3_schema.sql` (cole o conteúdo completo)
5. Vá em **Authentication → Users** → clique em **Add User**:
   - Rodrigo: `rodrigo@[dominio].com.br` → papel `admin`
   - Ana: `ana@[dominio].com.br` → papel `operador`
6. Após criar os usuários, execute no SQL Editor:
   ```sql
   UPDATE profiles SET papel = 'admin' WHERE email = 'rodrigo@[dominio].com.br';
   ```

### 2. Repositório GitHub

```bash
# Clone ou push para o repo existente
git clone https://github.com/rvgomessp-sudo/CRM.git
cd CRM
# Copie os arquivos do CRM V3 aqui
git add .
git commit -m "CRM V3 — rebuild completo com Supabase + Next.js"
git push origin main
```

**IMPORTANTE:** As bases CSV (.csv) estão no .gitignore — nunca commitar no repositório.

### 3. Variáveis de Ambiente

Copie `.env.local.example` para `.env.local`:

```bash
cp .env.local.example .env.local
```

Edite com os valores do Supabase:

```
NEXT_PUBLIC_SUPABASE_URL=https://[projeto].supabase.co
NEXT_PUBLIC_SUPABASE_ANON_KEY=eyJ...
```

### 4. Deploy Vercel

1. Acesse https://vercel.com/rvgomessp-sudos-projects
2. Conecte ao repositório GitHub `rvgomessp-sudo/CRM`
3. Em **Environment Variables**, adicione:
   - `NEXT_PUBLIC_SUPABASE_URL`
   - `NEXT_PUBLIC_SUPABASE_ANON_KEY`
4. Framework: **Next.js** (detectado automaticamente)
5. Clique em **Deploy**

### 5. Desenvolvimento local

```bash
npm install
npm run dev
# Acesse http://localhost:3000
```

---

## Estrutura do Projeto

```
app/
  (auth)/login/         → Login Supabase
  (dashboard)/
    dashboard/          → KPIs + funil + SLA
    pipeline/           → Kanban 10 estágios
    base-pgfn/          → Tabela com filtros + busca
    empresa/[cnpj]/     → Ficha: inscrições, interações, proposta
    importar/           → CSV F1/F2 com dedup
    solver/             → VF Solver + geração de proposta
components/
  Sidebar.tsx           → Navegação lateral
lib/
  types.ts              → Todos os tipos TypeScript
  utils.ts              → CNPJ, BRL, datas, SLA
  motor.ts              → Classificação A1/A2/B1/B2
  supabase/client.ts    → Cliente browser
  supabase/server.ts    → Cliente server (App Router)
middleware.ts           → Auth guard + redirect
crm_v3_schema.sql       → Schema completo PostgreSQL
```

---

## Regras críticas — não alterar

### CNPJ Raiz
```typescript
// Remove TODOS os não-numéricos, pega os 8 primeiros dígitos
const raiz = cnpj.replace(/\D/g, '').substring(0, 8).padStart(8, '0')
```

### Classificação de Motores
| Motor | Lógica | Abordagem |
|-------|--------|-----------|
| A1 | Ajuizado + SEM garantia | Execução ativa, risco BACENJUD |
| A2 | NÃO ajuizado + SEM garantia | Antecipe antes do ajuizamento |
| B1 | Situação contém PENHORA | Substituição por SG |
| B2 | Situação contém SEGURO GARANTIA | Revisão de prêmio |

**Prioridade empresa:** A1 > B1 > B2 > A2

### Regra econômica V&F
`comissão + honorários ≥ R$ 80.000` (teto mínimo)
`comissão + honorários ≤ R$ 200.000` (teto-alvo)

### Dedup na importação
- Empresa: `ON CONFLICT (cnpj_raiz) DO UPDATE` — atualiza sempre
- Inscrição: `ON CONFLICT (numero_inscricao) DO UPDATE` — atualiza sempre
- Motor da empresa: calculado automaticamente via trigger SQL

---

## Usuários

| Usuário | Papel | Escopo |
|---------|-------|--------|
| Rodrigo | Admin | Análise técnica, financeiro, sem limite |
| Ana | Operador | Follow-up, institucional, pós-reunião |

---

## Próximos passos (Fase 2)

- [ ] Integração CNPJ.gov.br (enriquecimento automático)
- [ ] Template de proposta PDF
- [ ] Notificações SLA por e-mail
- [ ] API Sancor (consulta de cadastro)
- [ ] OSINT (Hunter.io, LinkedIn) — com aprovação humana
- [ ] Exportação Excel da carteira

<!-- deploy trigger: 2026-08-17 dossie rose gold -->
