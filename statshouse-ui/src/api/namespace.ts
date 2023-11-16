// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { apiFetch } from './api';
import { API_FETCH_OPT_METHODS, GET_PARAMS } from './enum';

const ApiNamespaceEndpoint = '/api/namespace';

/**
 * Response endpoint api/group
 */
export type ApiNamespace = {
  data: {
    namespace: Namespace;
  };
};
export type Namespace = {
  namespace_id: number;
  name: string;
  version: number;
  update_time: number;
  delete_time: number;
  weight: number;
  disable: boolean;
};

export type ApiNamespaceGet = {
  [GET_PARAMS.metricsNamespacesID]: string;
};

export type ApiNamespacePost = {
  namespace: {
    namespace_id: number;
    name: string;
    weight: number;
    version: number;
    disable?: boolean;
  };
};
export type ApiNamespacePut = {
  namespace: {
    name: string;
    weight: number;
  };
};

export async function apiNamespaceFetch(params: ApiNamespaceGet, keyRequest?: unknown) {
  return await apiFetch<ApiNamespace>({ url: ApiNamespaceEndpoint, get: params, keyRequest });
}
export async function apiPutNamespaceFetch(params: ApiNamespacePut, keyRequest?: unknown) {
  return await apiFetch<ApiNamespace>({
    url: ApiNamespaceEndpoint,
    post: params,
    method: API_FETCH_OPT_METHODS.put,
    keyRequest,
  });
}
export async function apiPostNamespaceFetch(params: ApiNamespacePost, keyRequest?: unknown) {
  return await apiFetch<ApiNamespace>({
    url: ApiNamespaceEndpoint,
    post: params,
    keyRequest,
  });
}
