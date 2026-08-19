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
        // Garantia sem Barreiras v7 — COBRE/DOURADO sobre OXBLOOD (vinho profundo)
        // Marca GSB · Monitor Judicial · Grupo V&F. Ref: logo GB + mockup GSB Monitor.
        bg:       { primary: '#1A0B0B', secondary: '#22100E', card: '#2B1614', hover: '#38201C' },
        border:   { DEFAULT: '#4A2822', strong: '#6B3D31' },
        // vf-red*/gold/rose mantidos por compatibilidade de classe — renderizam COBRE/DOURADO
        vf:       { red: '#C98A54', 'red-dark': '#A66B3C', 'red-light': '#E8C39A' },
        gold:     { DEFAULT: '#C98A54', dark: '#A66B3C', light: '#E8C39A' },
        rose:     { DEFAULT: '#C98A54', dark: '#A66B3C', light: '#E8C39A', deep: '#8A5330' },
        text:     { primary: '#F3E9E2', muted: '#B49B92', faint: '#7C6058' },
        success:  '#45C98E',
        warning:  '#E8B14A',
        info:     '#8AB0DE',
        danger:   '#FF6B6B',
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
