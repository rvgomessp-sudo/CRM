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
        // Garantia sem Barreiras v6 — COBRE sobre AZUL-MARINHO PROFUNDO
        // (Fase 17 da ordem executiva 19/08; referência: mockups GSB)
        bg:       { primary: '#0A1322', secondary: '#0F1B2E', card: '#142238', hover: '#1B2C47' },
        border:   { DEFAULT: '#233650', strong: '#33496B' },
        // vf-red*/gold/rose mantidos por compatibilidade de classe — renderizam COBRE
        vf:       { red: '#D89B78', 'red-dark': '#B47A55', 'red-light': '#EDC1A4' },
        gold:     { DEFAULT: '#D89B78', dark: '#B47A55', light: '#EDC1A4' },
        rose:     { DEFAULT: '#D89B78', dark: '#B47A55', light: '#EDC1A4', deep: '#9E6743' },
        text:     { primary: '#EDF1F7', muted: '#94A4BA', faint: '#566A85' },
        success:  '#3BCE93',
        warning:  '#E2AC42',
        info:     '#6FA8E8',
        danger:   '#F0616B',
        // Motor colors
        motor: {
          A1: '#F0616B',  // vermelho-rosé urgente
          A2: '#E0A93C',  // âmbar
          B1: '#6FA8E8',  // azul
          B2: '#B08CC9',  // violeta suave
        },
        // Pipeline stage colors — progressão fria→rose gold→verde (conversão)
        stage: {
          'base-pgfn': '#574C57',
          enriquecimento: '#6FA8E8',
          abordagem: '#E0A93C',
          interesse: '#4FB89A',
          analise: '#B08CC9',
          proposta: '#DDA096',
          sancor: '#BC8073',
          aprovado: '#35C98E',
          fechado: '#2A9D74',
          receita: '#1F7A58',
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
