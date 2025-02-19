// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { join } from 'path';

import { defineConfig, loadEnv, type UserConfig } from 'vite';
import ReactSWC from '@vitejs/plugin-react-swc';
import { ViteEjsPlugin } from 'vite-plugin-ejs';
import SVGRRollup from '@svgr/rollup';
import eslintPlugin from 'vite-plugin-eslint';

// https://vitejs.dev/config/
export default defineConfig(async (init) => {
  const { REACT_APP_PROXY, REACT_APP_PROXY_COOKIE, REACT_APP_DEV_PORT, REACT_APP_DEV_HOST } = loadEnv(
    init.mode,
    process.cwd(),
    ['REACT_']
  );
  const port = parseInt(REACT_APP_DEV_PORT) || 3000;
  const headers: { [header: string]: string } = REACT_APP_PROXY_COOKIE ? { cookie: REACT_APP_PROXY_COOKIE } : {};
  const config: UserConfig = {
    base: '/',
    root: './',
    build: {
      sourcemap: true,
      assetsInlineLimit: 0,
      outDir: 'build',
      chunkSizeWarningLimit: 1000000,
    },
    plugins: [
      ViteEjsPlugin((viteConfig) => ({
        env: viteConfig.env,
      })),
      ReactSWC({
        devTarget: 'es2020',
      }),
      SVGRRollup({
        typescript: true,
        svgo: true,
        svgoConfig: {
          plugins: [
            {
              name: 'preset-default',
              params: {
                overrides: {
                  removeViewBox: false,
                },
              },
            },
          ],
        },
      }),
      eslintPlugin(),
    ],
    resolve: {
      alias: [
        {
          find: /@\//,
          replacement: join(process.cwd(), './src') + '/',
        },
      ],
    },
    server: {
      port,
      host: REACT_APP_DEV_HOST || undefined,
      proxy: {
        '/api': {
          target: REACT_APP_PROXY,
          changeOrigin: true,
          headers,
        },
      },
      open: '/',
    },
    preview: {
      port,
      host: REACT_APP_DEV_HOST || undefined,
      proxy: {
        '/api': {
          target: REACT_APP_PROXY,
          changeOrigin: true,
          headers,
        },
      },
      open: '/',
    },
    envPrefix: ['VITE_', 'REACT_'],
    esbuild: { logOverride: { 'negative-zero-esbuild-warn': 'silent' } },
  };
  return config;
});
