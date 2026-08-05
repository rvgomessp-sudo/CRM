/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    './pages/**/*.{js,ts,jsx,tsx,mdx}',
    './components/**/*.{js,ts,jsx,tsx,mdx}',
    './app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors: {
        // V&F Brand — Grafite/Branco/Vermelho Cereja
        bg:       { primary: '#0D0E13', secondary: '#161821', card: '#1C1F2E', hover: '#222537' },
        border:   { DEFAULT: '#2A2D3A', strong: '#3E4256' },
        vf:       { red: '#C41E3A', 'red-dark': '#9B1630', 'red-light': '#E53E5C' },
        text:     { primary: '#F0F2F5', muted: '#8B91A1', faint: '#4A5068' },
        success:  '#22C55E',
        warning:  '#F59E0B',
        info:     '#3B82F6',
        danger:   '#EF4444',
        // Motor colors
        motor: {
          A1: '#EF4444',  // vermelho urgente
          A2: '#F59E0B',  // âmbar
          B1: '#3B82F6',  // azul
          B2: '#8B5CF6',  // violeta
        },
        // Pipeline stage colors
        stage: {
          'base-pgfn': '#4A5068',
          enriquecimento: '#0EA5E9',
          abordagem: '#F59E0B',
          interesse: '#10B981',
          analise: '#8B5CF6',
          proposta: '#C41E3A',
          sancor: '#E11D48',
          aprovado: '#22C55E',
          fechado: '#15803D',
          receita: '#166534',
        },
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'Fira Code', 'monospace'],
      },
      borderRadius: {
        DEFAULT: '6px',
        lg: '10px',
        xl: '14px',
      },
    },
  },
  plugins: [],
}
