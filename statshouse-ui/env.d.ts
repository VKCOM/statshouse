/// <reference types="vite/client" />
interface ImportMetaEnv {
  readonly VITE_PROXY: string;
  readonly REACT_APP_PROXY: string;
  readonly REACT_APP_PROXY_COOKIE: string;
  readonly REACT_APP_CONFIG: string;
  readonly REACT_APP_BUILD_VERSION: string;
  readonly REACT_APP_DEV_PORT: string;
  readonly REACT_APP_DEV_HOST: string;
  // more env variables...
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
