// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { hasOwn, toFlatPairs, toString } from '../common/helpers';
import { API_FETCH_OPT_METHODS, ApiFetchOptMethods } from './enum';

export type ApiFetchParam = Record<string, unknown | unknown[]>;

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
  error?: ExtendedError;
};

export class ApiAbortController extends AbortController {
  static abortControllers: Map<unknown, AbortController> = new Map();

  timer?: NodeJS.Timeout;
  keyRequest?: unknown;

  constructor(keyRequest?: unknown, timeout: number = 300000) {
    super();
    this.keyRequest = keyRequest;
    this.signal.addEventListener('abort', this.remove);
    const abortFn = () => {
      this.abort();
    };

    this.timer = timeout ? setTimeout(abortFn, timeout) : undefined;

    let outerSignal: AbortSignal | undefined;
    if (keyRequest instanceof AbortController) {
      outerSignal = keyRequest.signal;
    }

    if (keyRequest instanceof AbortSignal) {
      outerSignal = keyRequest;
    }

    outerSignal?.addEventListener('abort', abortFn);
    if (keyRequest) {
      ApiAbortController.abortControllers.get(keyRequest)?.abort();
      ApiAbortController.abortControllers.set(keyRequest, this);
    }
  }

  remove() {
    clearTimeout(this.timer);
    this.signal?.removeEventListener('abort', this.remove);
    ApiAbortController.abortControllers.delete(this.keyRequest);
  }
}

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

  const controller = new ApiAbortController(keyRequest);

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
      result.error = new ExtendedError(`${resp.status}: ${text.substring(0, 255)}`, resp.status);
    } else {
      const json = (await resp.json()) as R;
      if (resp.ok) {
        result.ok = true;
        result.response = json;
      } else if (hasOwn(json, 'error') && typeof json.error === 'string') {
        result.error = new ExtendedError(json.error, resp.status);
      }
    }
  } catch (error) {
    result.status = ExtendedError.ERROR_STATUS_UNKNOWN;
    if (error instanceof Error) {
      if (error.name !== 'AbortError') {
        result.error = new ExtendedError(error);
      } else {
        result.error = new ExtendedError(error, ExtendedError.ERROR_STATUS_ABORT);
        result.status = ExtendedError.ERROR_STATUS_ABORT;
      }
    } else {
      result.error = new ExtendedError('Fetch error');
    }
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

export class ExtendedError extends Error {
  static readonly ERROR_STATUS_UNKNOWN = -1;
  static readonly ERROR_STATUS_ABORT = -2;

  status: number = ExtendedError.ERROR_STATUS_UNKNOWN;

  constructor(message: string | Error | unknown, status: number = ExtendedError.ERROR_STATUS_UNKNOWN) {
    super(message instanceof Error ? '' : typeof message === 'string' ? message : 'unknown error');
    if (message instanceof Error) {
      Object.assign(this, message);
    } else if (typeof message !== 'string') {
      this.cause = message;
    }
    this.status = status;
  }
  toString() {
    return `${this.status}: ${this.message ? this.message.substring(0, 255) : 'unknown error'}`;
  }
}
