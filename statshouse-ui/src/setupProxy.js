const { createProxyMiddleware } = require('http-proxy-middleware');

module.exports = function (app) {
  if (process.env.REACT_APP_PROXY) {
    app.use(
      '/api',
      createProxyMiddleware({
        target: process.env.REACT_APP_PROXY,
        changeOrigin: true,
        onProxyReq: (proxyReq) => {
          if (process.env.REACT_APP_PROXY_COOKIE) {
            proxyReq.setHeader('cookie', process.env.REACT_APP_PROXY_COOKIE);
          }
        },
      })
    );
  }
};
