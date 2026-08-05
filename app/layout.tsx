import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'CRM V3 | Vazquez & Fonseca',
  description: 'Pipeline PGFN — Seguro Garantia Tributário',
  robots: 'noindex, nofollow',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="pt-BR" suppressHydrationWarning>
      <body>{children}</body>
    </html>
  )
}
