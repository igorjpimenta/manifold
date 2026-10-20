import type { Config } from "tailwindcss";

// Palette mirrors the previous vanilla styles.css so the React + Tailwind
// dashboard matches the established look (dark theme, GitHub-ish accents).
// Status colors keep their names so existing JSX class strings read like
// English: `bg-status-succeeded text-status-succeeded` etc.
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        bg: {
          DEFAULT: "#0e1116",
          elev: "#161b22",
          elev2: "#1f262e",
        },
        border: {
          DEFAULT: "#2a3038",
        },
        fg: {
          DEFAULT: "#d8dde6",
          dim: "#8b949e",
        },
        accent: "#58a6ff",
        status: {
          green: "#3fb950",
          amber: "#d29922",
          red: "#f85149",
          blue: "#1f6feb",
          grey: "#6e7681",
        },
      },
      fontFamily: {
        mono: ['ui-monospace', '"SF Mono"', 'Menlo', 'Consolas', 'monospace'],
      },
      fontSize: {
        "2xs": "10px",
      },
    },
  },
  plugins: [],
} satisfies Config;
