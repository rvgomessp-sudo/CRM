# Runbook de Deploy — VF CRM v2.0

Guia passo-a-passo para colocar o CRM rodando na internet (Supabase + Railway + GitHub Pages).

**Custo total:** R$ 30-150/mês.
**Tempo estimado:** 1-2 horas para a primeira vez.

---

## Pré-requisitos

- Conta GitHub (já existe: `rvgomessp-sudo/CRM`)
- Conta de email para criar contas em Supabase, Railway, Sentry
- Cartão de crédito (Railway exige cadastro mas tem free tier de teste)

---

## Passo 1 — Banco de dados (Supabase)

### 1.1 Criar projeto

1. Acesse https://supabase.com → **Sign up** (use GitHub para login rápido)
2. **New Project**
3. Preencha:
   - **Name:** `vf-crm`
   - **Database Password:** gere uma senha forte, **anote em local seguro** (será o `DB_PASSWORD`)
   - **Region:** `South America (São Paulo)` para menor latência
   - **Pricing Plan:** `Free` (500MB grátis — suficiente para começar)
4. Clique **Create new project** e aguarde ~2 minutos

### 1.2 Pegar a connection string

1. No projeto criado → **Settings** (engrenagem) → **Database**
2. Role até **Connection string** → aba **URI**
3. Copie a string. Será algo como:
   ```
   postgresql://postgres:[YOUR-PASSWORD]@db.abcdefghij.supabase.co:5432/postgres
   ```
4. Substitua `[YOUR-PASSWORD]` pela senha que você anotou
5. **Anote esta URL completa** — será o `DATABASE_URL` no Railway

---

## Passo 2 — Backend (Railway)

### 2.1 Criar projeto

1. Acesse https://railway.app → **Login with GitHub**
2. **New Project** → **Deploy from GitHub repo**
3. Autorize Railway a acessar `rvgomessp-sudo/CRM`
4. Selecione o repositório `CRM`
5. Selecione a branch `claude/setup-vf-crm-v2-N5nc0` (ou faça merge para `main` antes)
6. Railway detecta o `railway.toml` e o `Dockerfile` automaticamente

### 2.2 Configurar variáveis de ambiente

No projeto Railway → **Variables** → adicione todas:

| Variável | Valor |
|---|---|
| `DATABASE_URL` | a URL do Supabase do passo 1.2 |
| `SECRET_KEY` | gere com `openssl rand -hex 32` (32+ caracteres aleatórios) |
| `AUTH_REQUIRED` | `true` |
| `ACCESS_TOKEN_EXPIRE_HOURS` | `12` |
| `CORS_ORIGINS` | `https://crm.vazquezefonseca.com.br` (ou `*` se ainda não tem domínio) |
| `ENVIRONMENT` | `production` |
| `SKIP_INIT_DB` | `true` (Alembic vai gerenciar o schema) |
| `SENTRY_DSN` | (opcional, ver passo 5) |

### 2.3 Deploy

Railway faz deploy automático. Acompanhe em **Deployments** → **View Logs**.

Você verá:
```
Running upgrade -> 190b01228c37, initial schema
INFO: Uvicorn running on http://0.0.0.0:8000
```

Quando aparecer "Application startup complete", está no ar.

### 2.4 Pegar a URL pública

Em **Settings** → **Networking** → **Generate Domain**. Vai gerar algo como:
```
vf-crm-production.up.railway.app
```

Teste: `https://vf-crm-production.up.railway.app/api/health` deve retornar `{"status":"ok",...}`

### 2.5 Criar o usuário admin (bootstrap)

```bash
curl -X POST https://vf-crm-production.up.railway.app/api/auth/bootstrap \
  -H "Content-Type: application/json" \
  -d '{"username":"rodrigo","nome_completo":"Rodrigo Vazquez","senha":"SUA_SENHA_FORTE","papel":"admin"}'
```

**Importante:** depois disso, o endpoint `/bootstrap` é desativado automaticamente. Para criar a Anna, faça login como rodrigo e use `/api/auth/users` (admin only).

---

## Passo 3 — Frontend (GitHub Pages ou Railway Static)

### Opção A: GitHub Pages (mais simples, grátis)

1. No repo GitHub → **Settings** → **Pages**
2. **Source:** Deploy from a branch
3. **Branch:** `main` (ou outra de produção) → `/frontend`
4. **Save**
5. Aguarde 1-2 minutos. URL será `https://rvgomessp-sudo.github.io/CRM/VF_CRM.html`

**Importante:** edite o arquivo `frontend/VF_CRM.html` e troque a constante:
```javascript
const API_BASE = 'https://vf-crm-production.up.railway.app';
```
em vez do `localhost:8000`. Commit + push para atualizar.

### Opção B: Servir junto com o backend no Railway

Edite `backend/main.py` e adicione um StaticFiles mount apontando para `frontend/`. Mais simples para domínio único, mais trabalho para configurar.

---

## Passo 4 — Domínio (vazquezefonseca.com.br)

### 4.1 Subdomínio crm.

No painel DNS do seu provedor (Registro.br, Cloudflare, etc):

| Tipo | Nome | Valor |
|---|---|---|
| `CNAME` | `crm` | `vf-crm-production.up.railway.app` |

Aguarde 5-30 minutos para propagação.

### 4.2 Configurar no Railway

No Railway → **Settings** → **Networking** → **Custom Domain** → adicione `crm.vazquezefonseca.com.br`. Railway gera o certificado SSL automaticamente.

### 4.3 Atualizar CORS

No Railway → **Variables** → mude `CORS_ORIGINS` para:
```
https://crm.vazquezefonseca.com.br
```

E atualize o `API_BASE` no frontend para o mesmo domínio (ou use `''` para same-origin se servir frontend e backend juntos).

---

## Passo 5 — Sentry (monitoramento de erros) — opcional mas recomendado

1. https://sentry.io → Sign up (free tier: 5K eventos/mês)
2. **Create Project** → **FastAPI**
3. Copie o **DSN** que aparece na próxima tela
4. No Railway → **Variables** → adicione `SENTRY_DSN=https://...@sentry.io/...`
5. Railway faz redeploy automaticamente. Erros aparecerão no Sentry em tempo real.

---

## Passo 6 — Importar a base PGFN

Após o login funcionar, importe a base via interface (drag-and-drop em **Importar**) ou via curl:

```bash
TOKEN=$(curl -s -X POST https://crm.vazquezefonseca.com.br/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"rodrigo","senha":"SUA_SENHA"}' | python -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

curl -X POST https://crm.vazquezefonseca.com.br/api/companies/import \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@PGFN-4T2025.xlsx"
```

Resultado esperado: `{"importadas": 6693, "duplicatas": 3786, "erros": 0}`

---

## Manutenção

### Backup do banco

Supabase Free: snapshots manuais via dashboard.
Supabase Pro ($25/mês): backups diários automáticos com retenção de 7 dias.

### Atualizar o código

```bash
git push origin main
```
Railway detecta o push e faz redeploy automático em ~2 minutos.

### Adicionar uma coluna nova

```bash
# Localmente
alembic revision --autogenerate -m "add nova_coluna"
git add backend/migrations/
git commit -m "migration: add nova_coluna"
git push
```
Railway aplica a migração automaticamente no próximo deploy (Dockerfile CMD: `alembic upgrade head && uvicorn ...`).

### Logs em produção

Railway → **Deployments** → último deploy → **View Logs**. Logs em tempo real do uvicorn.

---

## Troubleshooting

| Problema | Solução |
|---|---|
| `502 Bad Gateway` no Railway | Veja os logs. Provavelmente erro na inicialização. |
| `Could not validate credentials` | Token JWT expirou. Faça login novamente. |
| `database connection failed` | DATABASE_URL errada ou Supabase pausado (free tier pausa após 7 dias sem uso) |
| `CORS policy: No 'Access-Control-Allow-Origin'` | CORS_ORIGINS não inclui o domínio do frontend |
| Migração falha | Rode `alembic current` para ver estado, `alembic upgrade head` manualmente para tentar |

---

## Custo mensal estimado (go-live mínimo)

| Serviço | Plano | Custo |
|---|---|---|
| Supabase | Free → Pro | $0 → $25 |
| Railway | Hobby | ~$5 |
| Domínio (já têm) | — | ~R$ 50/ano |
| Sentry | Free | $0 |
| **Total** | | **R$ 30-150/mês** |
