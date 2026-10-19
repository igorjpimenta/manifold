import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "node:path";

// Built under base `/graph/` so the dashboard can be mounted as a subpath
// behind tailscale serve (e.g. https://<tailnet>/graph/) without asset paths
// colliding with the gateway at `/`. server.js strips the prefix for direct
// loopback requests, and tailscale serve strips it on the proxy side, so the
// same bundle works both ways.
export default defineConfig({
  base: "/graph/",
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
    rollupOptions: {
      output: {
        manualChunks: undefined,
      },
    },
  },
  server: {
    port: 5173,
    proxy: {
      "/graph/api":    { target: "http://127.0.0.1:7430", changeOrigin: false },
      "/graph/static": { target: "http://127.0.0.1:7430", changeOrigin: false },
    },
  },
});
