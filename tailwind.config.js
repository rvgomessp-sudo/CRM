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
        // Garantia sem Barreiras v5 — Rose Gold sobre noite mauve (vermelho reservado a ALERTA)
        bg:       { primary: '#0C0A0E', secondary: '#121016', card: '#18141E', hover: '#1F1A26' },
        border:   { DEFAULT: '#241F2B', strong: '#37303F' },
        // vf-red* e gold mantidos por compatibilidade de classe — agora renderizam ROSE GOLD
        vf:       { red: '#DDA096', 'red-dark': '#BC8073', 'red-light': '#F0C4BA' },
        gold:     { DEFAULT: '#DDA096', dark: '#BC8073', light: '#F0C4BA' },
        rose:     { DEFAULT: '#DDA096', dark: '#BC8073', light: '#F0C4BA', deep: '#A66B5C' },
        text:     { primary: '#F1ECEF', muted: '#9A8F97', faint: '#574C57' },
        success:  '#35C98E',
        warning:  '#E0A93C',
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
