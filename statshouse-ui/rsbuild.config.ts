import { pluginReact } from '@rsbuild/plugin-react';
import { pluginSvgr } from '@rsbuild/plugin-svgr';
import { pluginSass } from '@rsbuild/plugin-sass';
import { defineConfig, loadEnv, RsbuildConfig } from '@rsbuild/core';

export default defineConfig(async ({ envMode }): Promise<RsbuildConfig> => {
  const { parsed, publicVars } = loadEnv({ mode: envMode, cwd: process.cwd(), prefixes: ['REACT_'] });
  const { REACT_APP_PROXY, REACT_APP_PROXY_COOKIE, REACT_APP_DEV_PORT, REACT_APP_DEV_HOST } = parsed;
  return {
    output: {
      distPath: {
        root: 'build',
        js: 'assets',
        css: 'assets',
      },
      sourceMap: {
        js: 'source-map',
        css: true,
      },
      module: true,
    },
    performance: {
      chunkSplit: {
        strategy: 'split-by-experience',
      },
      bundleAnalyze: {},
    },
    tools: {
      rspack: {
        optimization: {
          splitChunks: {
            chunks: 'async',
          },
        },
      },
    },
    html: {
      scriptLoading: 'module',
      template: './index.ejs',
    },
    source: {
      define: publicVars,
    },
    plugins: [
      pluginReact({}),
      pluginSass(),
      pluginSvgr({
        svgrOptions: {
          exportType: 'named',
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
        },
      }),
    ],
    server: {
      open: !process.env.CI,
      port: Number(REACT_APP_DEV_PORT) || undefined,
      host: REACT_APP_DEV_HOST || undefined,
      proxy: {
        '/api': {
          target: REACT_APP_PROXY,
          changeOrigin: true,
          secure: false,
          onProxyReq: (proxyReq) => {
            if (REACT_APP_PROXY_COOKIE) {
              proxyReq.setHeader('COOKIE', REACT_APP_PROXY_COOKIE);
            }
          },
        },
      },
    },
  };
});
