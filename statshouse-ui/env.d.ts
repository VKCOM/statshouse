// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
