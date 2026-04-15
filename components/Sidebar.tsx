'use client'
// components/Sidebar.tsx
import Link from 'next/link'
import { usePathname, useRouter } from 'next/navigation'
import { createClient } from '@/lib/supabase/client'

const NAV = [
  { href: '/dashboard', label: 'Dashboard', icon: '▦' },
  { href: '/pipeline', label: 'Pipeline', icon: '⠿' },
  { href: '/base', label: 'Base PGFN', icon: '≡' },
  { href: '/importar', label: 'Importar', icon: '↑' },
  { href: '/solver', label: 'VF Solver', icon: '◈' },
  { href: '/configuracoes', label: 'Configurações', icon: '⚙' },
]

export default function Sidebar() {
  const pathname = usePathname()
  const router = useRouter()

  async function handleLogout() {
    const supabase = createClient()
    await supabase.auth.signOut()
    router.push('/login')
  }

  return (
    <div className="sidebar">
      {/* Logo */}
      <div style={{ padding: '0 20px 20px', borderBottom: '1px solid var(--color-border)', marginBottom: 8 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <div style={{
            width: 32, height: 32,
            background: 'var(--color-primary)',
            borderRadius: 7,
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            fontSize: 13, fontWeight: 800, color: 'white'
          }}>VF</div>
          <div>
            <div style={{ fontWeight: 700, fontSize: 14 }}>V&F CRM</div>
            <div style={{ fontSize: 10, color: 'var(--color-text-muted)' }}>PGFN v3.0</div>
          </div>
        </div>
      </div>

      {/* Nav */}
      <nav style={{ flex: 1 }}>
        {NAV.map(item => (
          <Link
            key={item.href}
            href={item.href}
            className={`nav-item ${pathname.startsWith(item.href) ? 'active' : ''}`}
          >
            <span style={{ fontSize: 14, width: 18, textAlign: 'center' }}>{item.icon}</span>
            {item.label}
          </Link>
        ))}
      </nav>

      {/* Logout */}
      <div style={{ padding: '20px 0 0', borderTop: '1px solid var(--color-border)', marginTop: 'auto' }}>
        <button
          onClick={handleLogout}
          className="nav-item"
          style={{ width: '100%', background: 'none', border: 'none', textAlign: 'left' }}
        >
          <span style={{ fontSize: 14, width: 18, textAlign: 'center' }}>⏻</span>
          Sair
        </button>
      </div>
    </div>
  )
}
