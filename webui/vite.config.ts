import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';
import { resolve } from 'path'

export default defineConfig({
  plugins: [vue()],
  server: {
    port: 5173,
    proxy: {
      '/datahub01webApp': {
        target: 'http://localhost:29012',
        changeOrigin: true,
        secure: false,
      },
      '/api': {
        target: 'http://localhost:29012',
        changeOrigin: true,
        secure: false,
      }
    }
  },
  build: {
    lib: {
      entry: resolve(__dirname, 'src/main.ts'),
      name: 'datahubRustJniCache',
      formats: ['umd'],
      fileName: 'datahub-rust-jni-cache'
    },
    rollupOptions: {
      external: ['jquery'],
      output: {
        globals: {
          vue: 'Vue'
        }
      }
    }
  },
  define: {
    'process.env.NODE_ENV': JSON.stringify('production')
  }
});
