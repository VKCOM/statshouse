// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { apiFetch } from './api';

const ApiStatEndpoint = '/api/stat';
/**
 * Response endpoint api/stat
 */
export type ApiStat = {};

/**
 * Get params endpoint api/stat
 */
export type ApiStatGet = {};

/**
 * Post params endpoint api/stat
 */
export type ApiStatPost = {};

export async function apiStatFetch(params: ApiStatPost, keyRequest?: unknown) {
  return await apiFetch<ApiStat>({ url: ApiStatEndpoint, post: params, keyRequest });
}

const environment = process.env.NODE_ENV;
const UIErrorMetrickName = '__ui_errors';
export async function logError(event: ErrorEvent) {
  const fileName = event.filename.split('/').slice(-1)[0];
  const message = `${fileName}:${event.lineno}:${event.colno} ${event.error?.name}: ${event.error?.message}`;
  await apiStatFetch({
    metrics: [
      {
        ts: Math.floor(Date.now() / 1000),
        name: UIErrorMetrickName,
        tags: {
          1: environment,
          _s: message,
        },
        counter: 1,
        value: [1],
      },
    ],
  });
}

window.addEventListener('error', logError, false);
