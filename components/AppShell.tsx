'use client'

import { useState } from 'react'
import Sidebar from './Sidebar'
import { cn } from '@/lib/utils'
import { Menu, X } from 'lucide-react'

/**
 * Shell responsivo: sidebar estática no desktop (lg+), drawer deslizante
 * no mobile com barra superior e hambúrguer. A marca GB aparece na barra.
 */
export default function AppShell({
  userName, userRole, children,
}: { userName?: string; userRole?: string; children: React.ReactNode }) {
  const [open, setOpen] = useState(false)

  return (
    <div className="flex min-h-screen">
      {/* Sidebar — estática no desktop, drawer no mobile */}
      <div className={cn(
        'fixed inset-y-0 left-0 z-50 lg:static lg:z-auto transition-transform duration-200 ease-out',
        open ? 'translate-x-0' : '-translate-x-full lg:translate-x-0',
      )}>
        <Sidebar userName={userName} userRole={userRole} onNavigate={() => setOpen(false)} />
      </div>

      {/* Backdrop do drawer (só mobile) */}
      {open && (
        <div className="fixed inset-0 bg-black/60 z-40 lg:hidden" onClick={() => setOpen(false)} aria-hidden />
      )}

      <div className="flex-1 flex flex-col min-w-0">
        {/* Barra superior — só mobile */}
        <div className="lg:hidden sticky top-0 z-30 flex items-center gap-3 h-14 px-4 border-b border-border bg-bg-secondary">
          <button onClick={() => setOpen(true)} aria-label="Abrir menu"
            className="text-text-primary hover:text-vf-red-light transition-colors">
            <Menu className="w-5 h-5" />
          </button>
          <div className="flex items-center gap-2">
            <div className="w-7 h-7 rounded-full border-2 border-vf-red bg-bg-primary flex items-center justify-center"
                 style={{ boxShadow: '0 0 0 1px var(--accent-dark) inset' }}>
              <span className="font-serif font-bold text-vf-red-light text-[11px] tracking-tighter">GB</span>
            </div>
            <span className="text-text-primary font-semibold text-sm">GSB · Monitor Judicial</span>
          </div>
        </div>

        <main className="flex-1 flex flex-col min-w-0 overflow-auto bg-bg-primary">
          {children}
        </main>
      </div>
    </div>
  )
}
