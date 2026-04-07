# Quick Deploy — Tudo Grátis (sem domínio próprio)

Deploy do VF CRM em **3 passos**, tudo gratuito, sem usar `vazquezefonseca.com.br`.
**Tempo estimado:** 20 minutos.

URLs finais (exemplo):
- Frontend: `https://vf-crm.vercel.app`
- Backend: `https://vf-crm-production.up.railway.app`
- Banco: Supabase (interno, sem URL pública)

---

## Passo 1 — Banco de dados (Supabase) — 5 min

1. Acesse https://supabase.com → **Sign up** com GitHub
2. **New Project**:
   - **Name:** `vf-crm`
   - **Database Password:** gere uma senha forte → **anote**
   - **Region:** `South America (São Paulo)`
   - **Plan:** Free
3. Aguarde ~2 min até "Setting up project..." sumir
4. Vá em **Settings** (⚙️) → **Database** → role até **Connection string** → aba **URI**
5. Copie a string e substitua `[YOUR-PASSWORD]` pela senha que você anotou:
   ```
   postgresql://postgres:SUASENHA@db.abcdefghij.supabase.co:5432/postgres
   ```
6. **Guarde isso** — vai usar no Railway

---

## Passo 2 — Backend (Railway) — 10 min

### 2.1 Criar projeto

1. https://railway.app → **Login with GitHub**
2. **New Project** → **Deploy from GitHub repo** → autorize → selecione `rvgomessp-sudo/CRM`
3. Railway detecta o `railway.toml` e o `Dockerfile` automaticamente e começa o build
4. **NÃO espere terminar** — vá direto para as variáveis (item 2.2)

### 2.2 Configurar variáveis

No projeto Railway → clique no service criado → **Variables** → **+ New Variable** para cada:

| Variável | Valor |
|---|---|
| `DATABASE_URL` | a URL do Supabase do passo 1.5 |
| `SECRET_KEY` | qualquer string de 32+ caracteres aleatórios (use https://generate-secret.vercel.app/32) |
| `AUTH_REQUIRED` | `true` |
| `ACCESS_TOKEN_EXPIRE_HOURS` | `12` |
| `CORS_ORIGINS` | `*` (vamos restringir depois quando souber a URL do Vercel) |
| `ENVIRONMENT` | `production` |
| `SKIP_INIT_DB` | `true` |

Após salvar, Railway redeploya automaticamente.

### 2.3 Pegar URL pública

No service → **Settings** → role até **Networking** → **Generate Domain**.

Vai gerar algo como: `vf-crm-production-abc1.up.railway.app`

**Anote essa URL completa** (com `https://`).

### 2.4 Verificar que está no ar

Abra no browser:
```
https://vf-crm-production-abc1.up.railway.app/api/health
```

Deve retornar:
```json
{"status":"ok","version":"2.0.0","auth_required":true,"environment":"production"}
```

Se aparecer "502 Bad Gateway" ou erro: vá em **Deployments** → último deploy → **View Logs** e procure pelo erro (geralmente `DATABASE_URL` errada ou credentials do Supabase).

### 2.5 Criar usuário admin (bootstrap)

No terminal do seu PC:
```bash
curl -X POST https://vf-crm-production-abc1.up.railway.app/api/auth/bootstrap ^
  -H "Content-Type: application/json" ^
  -d "{\"username\":\"rodrigo\",\"nome_completo\":\"Rodrigo Vazquez\",\"senha\":\"SUA_SENHA_FORTE\"}"
```

(no Linux/Mac troque `^` por `\`)

Anote o username e senha — você vai usar para entrar no CRM.

---

## Passo 3 — Frontend (Vercel) — 5 min

1. https://vercel.com → **Login with GitHub**
2. Se você tem múltiplas teams, **selecione a team correta** no canto superior esquerdo
   (a sua: `team_4XaCJWa8SDRv4foUjMqP8OQn`)
3. **Add New** → **Project** → **Import Git Repository** → selecione `rvgomessp-sudo/CRM`
4. Configure:
   - **Project Name:** `vf-crm`
   - **Framework Preset:** `Other`
   - **Root Directory:** clique em **Edit** → digite `frontend` → **Continue**
   - **Build Command:** deixe em branco
   - **Output Directory:** deixe em branco (ou `.`)
   - **Install Command:** deixe em branco
5. Clique **Deploy**
6. Em ~30 segundos vai aparecer "Congratulations" com a URL: `https://vf-crm.vercel.app` (ou similar)

### 3.1 Apertar a porca CORS

Volte ao Railway → **Variables** → edite `CORS_ORIGINS`:
```
https://vf-crm.vercel.app
```
(coloque a URL exata do Vercel). Salve. Railway redeploya em ~30s.

---

## Passo 4 — Primeiro acesso e import

1. Abra `https://vf-crm.vercel.app`
2. Vai aparecer a tela **"Configurar Backend"**
3. Cole a URL do Railway: `https://vf-crm-production-abc1.up.railway.app`
4. Clique **Salvar e Continuar**
5. Aparece a tela de **Login**
6. Entre com `rodrigo` / sua senha
7. Vá em **Importar** → arraste o arquivo `PGFN - 4T2025.xlsx`
8. Em ~10 segundos: "6.693 importadas, 3.786 consolidadas"
9. Vá em **Dashboard** — KPIs aparecem
10. Vá em **Pipeline** — Kanban com as empresas

**Pronto.** O CRM está rodando 24/7 na internet, gratuito, com login, com banco persistente.

---

## Compartilhar com a Anna

Mande para ela:
- URL: `https://vf-crm.vercel.app`
- Usuário e senha (criar via `/api/auth/users` logado como admin, ou tela de Settings — falta implementar)

Para criar a Anna agora, no terminal:
```bash
TOKEN=curl -s -X POST https://vf-crm-production-abc1.up.railway.app/api/auth/login -H "Content-Type: application/json" -d "{\"username\":\"rodrigo\",\"senha\":\"SUA_SENHA\"}" | jq -r .access_token

curl -X POST https://vf-crm-production-abc1.up.railway.app/api/auth/users ^
  -H "Authorization: Bearer $TOKEN" ^
  -H "Content-Type: application/json" ^
  -d "{\"username\":\"anna\",\"nome_completo\":\"Ana Fonseca\",\"senha\":\"SENHA_ANA\",\"papel\":\"user\"}"
```

A Anna abre `https://vf-crm.vercel.app`, configura o backend (mesma URL do Railway), entra com seu usuário.

---

## Custos

| Serviço | Plano | Custo |
|---|---|---|
| Supabase | Free (500 MB) | R$ 0 |
| Railway | Trial credit ($5/mês após cartão cadastrado) | R$ 0-25 |
| Vercel | Hobby | R$ 0 |
| **Total** | | **R$ 0-25/mês** |

**Limites do free tier:**
- Supabase Free: pausa após 7 dias sem uso (acessar 1x/semana resolve)
- Railway: $5 de crédito/mês — backend pequeno cabe
- Vercel: ilimitado para projetos pessoais

Quando passar do teste e quiser usar `crm.vazquezefonseca.com.br`, siga o `DEPLOY.md` (passos 4-5 — domínio + SSL).

---

## Troubleshooting

| Problema | Solução |
|---|---|
| Vercel "Failed to fetch" | CORS errado no Railway. Atualize `CORS_ORIGINS` com URL exata do Vercel |
| Railway "502" | Veja logs → 99% das vezes é `DATABASE_URL` errada |
| Supabase pausou | Acesse o dashboard do Supabase 1x e clica em "Restore" |
| Login não funciona | Senha errada OU bootstrap não foi feito ainda |
| Bootstrap retorna 403 | Já foi feito antes. Use o usuário que você criou |
| Vercel não acha o frontend | Root Directory deve ser `frontend` (não vazio) |
