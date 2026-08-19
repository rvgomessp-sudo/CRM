'use client'

import { useState } from 'react'
import { useRouter } from 'next/navigation'
import { createClient } from '@/lib/supabase/client'

export const dynamic = 'force-dynamic'

export default function LoginPage() {
  const router = useRouter()

  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function handleLogin(e: React.FormEvent) {
    e.preventDefault()
    setLoading(true)
    setError(null)

    const supabase = createClient()
    const { error } = await supabase.auth.signInWithPassword({ email, password })

    if (error) {
      setError('Credenciais inválidas. Verifique e-mail e senha.')
      setLoading(false)
      return
    }

    router.push('/dashboard')
    router.refresh()
  }

  return (
    <div className="min-h-screen bg-bg-primary flex items-center justify-center px-4">
      <div className="w-full max-w-sm">

        {/* Marca GSB */}
        <div className="text-center mb-8">
          <div className="inline-flex flex-col items-center gap-2 mb-2">
            <div className="w-14 h-14 rounded-full border-2 border-vf-red bg-bg-secondary flex items-center justify-center"
                 style={{ boxShadow: '0 0 0 1px var(--accent-dark) inset' }}>
              <span className="font-serif font-bold text-vf-red-light text-xl tracking-tighter">GB</span>
            </div>
            <span className="text-text-primary font-semibold text-lg mt-1">GSB · Monitor Judicial</span>
          </div>
          <p className="text-vf-red-light text-[11px] uppercase tracking-widest">Garantia sem Barreiras · Grupo V&amp;F</p>
        </div>

        {/* Form */}
        <div className="card p-6">
          <h1 className="text-text-primary font-semibold mb-5">Acesso ao sistema</h1>

          {error && (
            <div className="mb-4 p-3 rounded bg-danger/10 border border-danger/30 text-danger text-sm">
              {error}
            </div>
          )}

          <form onSubmit={handleLogin} className="space-y-4">
            <div>
              <label className="label">E-mail</label>
              <input
                type="email"
                className="input"
                placeholder="rodrigo@..."
                value={email}
                onChange={e => setEmail(e.target.value)}
                required
                autoComplete="email"
              />
            </div>

            <div>
              <label className="label">Senha</label>
              <input
                type="password"
                className="input"
                placeholder="••••••••"
                value={password}
                onChange={e => setPassword(e.target.value)}
                required
                autoComplete="current-password"
              />
            </div>

            <button
              type="submit"
              className="btn-primary w-full justify-center py-2.5"
              disabled={loading}
            >
              {loading ? 'Autenticando…' : 'Entrar'}
            </button>
          </form>
        </div>

        <p className="text-center text-vf-red-light/80 text-xs italic mt-6">&ldquo;A inteligência sempre vence.&rdquo;</p>
        <p className="text-center text-text-faint text-[11px] mt-2">
          Uso restrito — Grupo V&amp;F © {new Date().getFullYear()}
        </p>
      </div>
    </div>
  )
}
