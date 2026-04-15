'use client'
// app/(painel)/configuracoes/page.tsx
import { useEffect, useState } from 'react'
import { createClient } from '@/lib/supabase/client'

export default function ConfiguracoesPage() {
  const [userInfo, setUserInfo] = useState<{ email?: string; id?: string } | null>(null)
  const [logs, setLogs] = useState<Record<string, string | number>[]>([])
  const supabase = createClient()

  useEffect(() => {
    supabase.auth.getUser().then(({ data }) => {
      setUserInfo({ email: data.user?.email, id: data.user?.id })
    })
    supabase
      .from('log_importacao')
      .select('*')
      .order('criado_em', { ascending: false })
      .limit(5)
      .then(({ data }) => setLogs(data || []))
  }, [])

  async function exportarBase() {
    const { data } = await supabase
      .from('empresas')
      .select('*, inscricoes(*)')
      .eq('excluido', false)
    if (!data) return
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `vf_crm_backup_${new Date().toISOString().split('T')[0]}.json`
    a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <div style={{ maxWidth: 700 }}>
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 20, fontWeight: 700, marginBottom: 4 }}>Configurações</h1>
        <p style={{ color: 'var(--color-text-muted)', fontSize: 13 }}>
          Conexão, usuários e backup da base
        </p>
      </div>

      {/* Conexão Supabase */}
      <div className="card" style={{ marginBottom: 16 }}>
        <h3 style={{ fontSize: 13, fontWeight: 600, marginBottom: 12, color: 'var(--color-text-muted)' }}>SUPABASE</h3>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between' }}>
            <span style={{ color: 'var(--color-text-muted)', fontSize: 13 }}>URL</span>
            <span style={{ fontFamily: 'monospace', fontSize: 12 }}>
              {process.env.NEXT_PUBLIC_SUPABASE_URL?.replace('https://', '').split('.')[0]}...supabase.co
            </span>
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between' }}>
            <span style={{ color: 'var(--color-text-muted)', fontSize: 13 }}>Status</span>
            <span style={{ color: '#10b981', fontSize: 13, fontWeight: 600 }}>● Conectado</span>
          </div>
        </div>
      </div>

      {/* Usuário logado */}
      <div className="card" style={{ marginBottom: 16 }}>
        <h3 style={{ fontSize: 13, fontWeight: 600, marginBottom: 12, color: 'var(--color-text-muted)' }}>SESSÃO ATIVA</h3>
        {userInfo && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span style={{ color: 'var(--color-text-muted)', fontSize: 13 }}>Email</span>
              <span style={{ fontSize: 13 }}>{userInfo.email}</span>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span style={{ color: 'var(--color-text-muted)', fontSize: 13 }}>User ID</span>
              <span style={{ fontFamily: 'monospace', fontSize: 11 }}>{userInfo.id}</span>
            </div>
          </div>
        )}
      </div>

      {/* Usuários do sistema */}
      <div className="card" style={{ marginBottom: 16 }}>
        <h3 style={{ fontSize: 13, fontWeight: 600, marginBottom: 4, color: 'var(--color-text-muted)' }}>USUÁRIOS DO SISTEMA</h3>
        <p style={{ fontSize: 12, color: 'var(--color-text-muted)', marginBottom: 12 }}>
          Acesso gerenciado via Supabase Auth + tabela usuarios. Para adicionar usuários, crie o email no Supabase Auth e insira na tabela usuarios com o auth_user_id correspondente.
        </p>
        <div style={{ display: 'flex', gap: 10 }}>
          <div style={{ padding: '8px 14px', background: 'var(--color-surface-2)', borderRadius: 6, fontSize: 13 }}>
            <strong>Rodrigo Vazquez</strong> — Admin · técnico/estruturador
          </div>
          <div style={{ padding: '8px 14px', background: 'var(--color-surface-2)', borderRadius: 6, fontSize: 13 }}>
            <strong>Ana</strong> — Operador · institucional/follow-up
          </div>
        </div>
      </div>

      {/* Regras de negócio */}
      <div className="card" style={{ marginBottom: 16, background: 'rgba(59,130,246,0.03)' }}>
        <h3 style={{ fontSize: 13, fontWeight: 600, marginBottom: 12, color: 'var(--color-text-muted)' }}>PARÂMETROS DA OPERAÇÃO</h3>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10, fontSize: 13 }}>
          {[
            ['Faixa principal', 'R$ 1M – R$ 5M (Sancor)'],
            ['Faixa ampliada', 'Até R$ 20M'],
            ['Comissão mínima', '20% sobre prêmio bruto'],
            ['Comissão máxima', '25% sobre prêmio bruto'],
            ['Taxa mínima Sancor', '0,50% a.a.'],
            ['Teto inicial', 'R$ 80K – R$ 120K'],
            ['Teto alvo', 'R$ 120K – R$ 200K'],
            ['Meta mensal', '10 – 20 operações'],
            ['SLA análise rápida', '1 dia útil'],
            ['SLA proposta', '48 horas'],
            ['Alerta parado', '> 7 dias sem movimento'],
            ['Regiões elegíveis', '3ª Região (SP/MS)'],
          ].map(([label, value]) => (
            <div key={label} style={{ display: 'flex', gap: 8 }}>
              <span style={{ color: 'var(--color-text-muted)', minWidth: 150 }}>{label}</span>
              <span style={{ fontWeight: 500 }}>{value}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Backup */}
      <div className="card" style={{ marginBottom: 16 }}>
        <h3 style={{ fontSize: 13, fontWeight: 600, marginBottom: 12, color: 'var(--color-text-muted)' }}>BACKUP</h3>
        <p style={{ fontSize: 12, color: 'var(--color-text-muted)', marginBottom: 12 }}>
          Exporta todas as empresas e inscrições em JSON. O Supabase mantém backups automáticos diários no plano Pro.
        </p>
        <button className="btn btn-ghost" onClick={exportarBase}>
          ↓ Exportar JSON completo
        </button>
      </div>

      {/* Últimas importações */}
      {logs.length > 0 && (
        <div className="card">
          <h3 style={{ fontSize: 13, fontWeight: 600, marginBottom: 12, color: 'var(--color-text-muted)' }}>ÚLTIMAS IMPORTAÇÕES</h3>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            {logs.map(log => (
              <div key={log.id as string} style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '6px 0', borderBottom: '1px solid var(--color-border)', fontSize: 12,
              }}>
                <span style={{ fontFamily: 'monospace', fontSize: 11 }}>{log.nome_arquivo}</span>
                <span style={{ color: 'var(--color-text-muted)' }}>
                  {log.empresas_novas} novas · {log.inscricoes_novas} inscr.
                </span>
                <span style={{ color: 'var(--color-text-muted)', fontSize: 11 }}>
                  {new Date(log.criado_em as string).toLocaleDateString('pt-BR')}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
