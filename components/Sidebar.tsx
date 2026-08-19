'use client'

import Link from 'next/link'
import { usePathname, useRouter } from 'next/navigation'
import { createClient } from '@/lib/supabase/client'
import {
  LayoutDashboard, KanbanSquare, Database,
  Calculator, LogOut, Shield, Target, CalendarCheck
} from 'lucide-react'
import { cn } from '@/lib/utils'

const NAV = [
  { href: '/pauta',         label: 'Pauta do dia',  icon: CalendarCheck },
  { href: '/oportunidades', label: 'Oportunidades', icon: Target },
  { href: '/pipeline',      label: 'Pipeline',      icon: KanbanSquare },
  { href: '/dashboard',     label: 'Dashboard',     icon: LayoutDashboard },
  { href: '/base-pgfn',     label: 'Base PGFN',     icon: Database },
  { href: '/solver',        label: 'VF Solver',     icon: Calculator },
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

      {/* Logo */}
      <div className="px-4 py-5 border-b border-border">
        <div className="flex items-center gap-2">
          <div className="w-7 h-7 bg-vf-red rounded flex items-center justify-center flex-shrink-0">
            <Shield className="w-3.5 h-3.5 text-white" />
          </div>
          <div>
            <p className="text-text-primary font-semibold text-sm leading-tight">V&F Grupo</p>
            <p className="text-vf-red-light text-[10px] uppercase tracking-widest">Originação por Eventos</p>
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
    </aside>
  )
}
