import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import commonjs from 'vite-plugin-commonjs'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), commonjs()],
  define: {
    'global': 'globalThis',
  },
  optimizeDeps: {
    include: ['google-protobuf'],
    esbuildOptions: {
      define: {
        global: 'globalThis'
      }
    }
  },
  build: {
    commonjsOptions: {
      include: [/google-protobuf/, /node_modules/, /src\/proto/],
      transformMixedEsModules: true
    },
    rollupOptions: {
      output: {
        manualChunks: {
          'proto': ['google-protobuf']
        }
      }
    }
  }
})
