// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { hasOwn, toFlatPairs, toString } from '../common/helpers';
import { API_FETCH_OPT_METHODS, ApiFetchOptMethods } from './enum';

export type ApiFetchParam = Record<string, any | any[]>;

export type ApiFetchOpt<G, P> = {
  url: string;
  get?: G;
  post?: P;
  method?: ApiFetchOptMethods;
  keyRequest?: unknown;
};

export type ApiFetchResponse<R> = {
  response?: R;
  status: number;
  ok: boolean;
  error?: Error;
};

const abortControllers: Map<unknown, AbortController> = new Map();

export async function apiFetch<R = unknown, G = ApiFetchParam, P = ApiFetchParam>({
  url,
  get,
  post,
  method,
  keyRequest,
}: ApiFetchOpt<G, P>): Promise<ApiFetchResponse<R>> {
  const result: ApiFetchResponse<R> = { ok: false, status: 0 };
  method ??= post ? API_FETCH_OPT_METHODS.post : API_FETCH_OPT_METHODS.get;
  const search = get ? '?' + new URLSearchParams(toFlatPairs(get, toString)).toString() : '';
  const fullUrl = url + search;
  const headers: HeadersInit = {};
  let body;
  if (post instanceof FormData) {
    body = post;
  } else if (post) {
    headers['Content-Type'] = 'application/json';
    body = JSON.stringify(post);
  }
  keyRequest ??= fullUrl;
  let controller: AbortController;
  if (keyRequest instanceof AbortController) {
    controller = keyRequest;
  } else {
    controller = new AbortController();
  }
  abortControllers.get(keyRequest)?.abort();
  abortControllers.set(keyRequest, controller);

  try {
    const resp = await fetch(fullUrl, {
      method,
      headers,
      signal: controller.signal,
      body,
    });
    result.status = resp.status;
    if (resp.headers.get('Content-Type') !== 'application/json') {
      const text = await resp.text();
      result.error = new Error(`${resp.status}: ${text.substring(0, 255)}`);
    } else {
      const json = (await resp.json()) as R;
      if (resp.ok) {
        result.ok = true;
        result.response = json;
      } else if (hasOwn(json, 'error') && typeof json.error === 'string') {
        result.error = new Error(json.error);
      }
    }
  } catch (error) {
    result.status = -1;
    if (error instanceof Error) {
      if (error.name !== 'AbortError') {
        result.error = error;
      } else {
        result.status = -2;
      }
    } else {
      result.error = new Error('Fetch error');
    }
  }
  if (result.status !== -2) {
    abortControllers.delete(keyRequest);
  }
  return result;
}

const firstFetchMap: Map<unknown, unknown> = new Map();

export async function apiFirstFetch<R = unknown, G = ApiFetchParam, P = ApiFetchParam>(
  params: ApiFetchOpt<G, P>
): Promise<ApiFetchResponse<R>> {
  let f: Promise<ApiFetchResponse<R>> | undefined;
  if (params.keyRequest) {
    f = firstFetchMap.get(params.keyRequest) as Promise<ApiFetchResponse<R>>;
  }
  if (!f) {
    f = apiFetch(params);
    firstFetchMap.set(params.keyRequest, f);
  }
  const result = await f;
  firstFetchMap.delete(params.keyRequest);
  return result;
}
