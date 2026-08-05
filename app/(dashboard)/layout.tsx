import { redirect } from 'next/navigation'
import { createClient } from '@/lib/supabase/server'
import Sidebar from '@/components/Sidebar'

export default async function DashboardLayout({ children }: { children: React.ReactNode }) {
  const supabase = createClient()
  const { data: { user } } = await supabase.auth.getUser()

  if (!user) redirect('/login')

  // Busca perfil
  const { data: profile } = await supabase
    .from('profiles')
    .select('nome, papel')
    .eq('id', user.id)
    .single()

  return (
    <div className="flex min-h-screen">
      <Sidebar userName={profile?.nome} userRole={profile?.papel} />
      <main className="flex-1 flex flex-col min-w-0 overflow-auto bg-bg-primary">
        {children}
      </main>
    </div>
  )
}
