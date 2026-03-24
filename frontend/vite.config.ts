import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const apiTarget =
    env.VITE_API_URL || 'http://127.0.0.1:8080'

  return {
    plugins: [react()],
    server: {
      proxy: {
        '/api': {
          // Default 8080 — Windows often blocks 8000 (WinError 10013 / Hyper-V reserved ranges).
          target: apiTarget,
          changeOrigin: true,
        },
      },
    },
  }
})
