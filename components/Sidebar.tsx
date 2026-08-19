'use client'

import Link from 'next/link'
import { usePathname, useRouter } from 'next/navigation'
import { createClient } from '@/lib/supabase/client'
import {
  LayoutDashboard, KanbanSquare, Database, Activity,
  Calculator, LogOut, Target, CalendarCheck
} from 'lucide-react'
import { cn } from '@/lib/utils'

const NAV = [
  { href: '/pauta',         label: 'Central',        icon: CalendarCheck },
  { href: '/oportunidades', label: 'Oportunidades',  icon: Target },
  { href: '/pipeline',      label: 'Pipeline',       icon: KanbanSquare },
  { href: '/dashboard',     label: 'Dashboard',      icon: LayoutDashboard },
  { href: '/monitoramento', label: 'Monitoramento',  icon: Activity },
  { href: '/base-pgfn',     label: 'Base PGFN',      icon: Database },
  { href: '/solver',        label: 'VF Solver',      icon: Calculator },
]

interface SidebarProps {
  userName?: string
  userRole?: string
}

export default function Sidebar({ userName, userRole }: SidebarProps) {
  const pathname = usePathname()
  const router = useRouter()
  const supabase = createClient()

  async function handleLogout() {
    await supabase.auth.signOut()
    router.push('/login')
    router.refresh()
  }

  return (
    <aside className="flex flex-col w-56 min-h-screen bg-bg-secondary border-r border-border">

      {/* Marca GSB — Garantia sem Barreiras */}
      <div className="px-4 py-5 border-b border-border">
        <div className="flex items-center gap-2.5">
          <div className="w-9 h-9 rounded-full border-2 border-vf-red bg-bg-primary flex items-center justify-center flex-shrink-0"
               style={{ boxShadow: '0 0 0 1px var(--accent-dark) inset' }}>
            <span className="font-serif font-bold text-vf-red-light text-[13px] tracking-tighter">GB</span>
          </div>
          <div>
            <p className="text-text-primary font-semibold text-sm leading-tight">GSB · Monitor Judicial</p>
            <p className="text-vf-red-light text-[10px] uppercase tracking-widest">Grupo V&amp;F</p>
          </div>
        </div>
      </div>

      {/* Nav */}
      <nav className="flex-1 px-2 py-4 space-y-0.5">
        {NAV.map(({ href, label, icon: Icon }) => {
          const active = pathname === href || pathname.startsWith(href + '/')
          return (
            <Link
              key={href}
              href={href}
              className={cn(
                'flex items-center gap-2.5 px-3 py-2 rounded text-sm transition-colors',
                active
                  ? 'bg-vf-red/10 text-vf-red-light font-medium'
                  : 'text-text-muted hover:bg-bg-hover hover:text-text-primary'
              )}
            >
              <Icon className="w-4 h-4 flex-shrink-0" />
              {label}
            </Link>
          )
        })}
      </nav>

      {/* User + logout */}
      <div className="px-2 py-3 border-t border-border">
        {userName && (
          <div className="px-3 py-2 mb-1">
            <p className="text-text-primary text-xs font-medium truncate">{userName}</p>
            <p className="text-text-faint text-[10px] capitalize">{userRole}</p>
          </div>
        )}
        <button
          onClick={handleLogout}
          className="flex items-center gap-2.5 w-full px-3 py-2 rounded text-sm
                     text-text-muted hover:text-danger hover:bg-danger/10 transition-colors"
        >
          <LogOut className="w-4 h-4" />
          Sair
        </button>
      </div>

      {/* Assinatura + frase da marca */}
      <div className="px-4 pt-3 pb-4 border-t border-border">
        <p className="text-vf-red-light/90 text-[11px] italic leading-snug">&ldquo;A inteligência sempre vence.&rdquo;</p>
        <p className="text-text-faint text-[9px] mt-2 uppercase tracking-widest">Concebido por</p>
        <p className="text-text-muted text-[13px]" style={{ fontFamily: 'Georgia, \"Times New Roman\", serif', fontStyle: 'italic' }}>Ana Fonseca</p>
      </div>
    </aside>
  )
}
