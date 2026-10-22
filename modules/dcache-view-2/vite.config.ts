import react from '@vitejs/plugin-react'
import { defineConfig } from 'vitest/config'

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      // keep Host: localhost:5173 so dCache rebuilds the OIDC redirect_uri
      // (and its post-login redirect) with the dev server's origin
      '/api': { target: 'http://localhost:3880', changeOrigin: false },
      '/scripts': 'http://localhost:3880',
    }
  },
  test: {
    environment: 'happy-dom',
  },
})
