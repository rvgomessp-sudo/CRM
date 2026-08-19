import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'GSB · Monitor Judicial | Grupo V&F',
  description: 'Garantia sem Barreiras — inteligência judicial para Seguro Garantia',
  robots: 'noindex, nofollow',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="pt-BR" suppressHydrationWarning>
      <body>{children}</body>
    </html>
  )
}
