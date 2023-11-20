// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { apiFetch } from './api';
import { API_FETCH_OPT_METHODS, GET_PARAMS } from './enum';

const ApiGroupEndpoint = '/api/group';

/**
 * Response endpoint api/group
 */
export type ApiGroup = {
  data: Group;
};
export type Group = {
  group: GroupInfo;
  metrics: GroupMetric[] | null;
};
export type GroupInfo = {
  group_id: number;
  namespace_id: number;
  name: string;
  version: number;
  update_time: number;
  weight: number;
  disable: boolean;
};

export type GroupMetric = string;

export type ApiGroupGet = {
  [GET_PARAMS.metricsGroupID]: string;
};

export type GetGroupListResp = {
  [GET_PARAMS.metricsGroupID]: string;
};

export type ApiGroupPost = {
  group: {
    group_id: number;
    namespace_id: number;
    name: string;
    weight: number;
    version: number;
    disable?: boolean;
  };
};
export type ApiGroupPut = {
  group: {
    name: string;
    weight: number;
  };
};

export async function apiGroupFetch(params: ApiGroupGet, keyRequest?: unknown) {
  return await apiFetch<ApiGroup>({ url: ApiGroupEndpoint, get: params, keyRequest });
}
export async function apiPutGroupFetch(params: ApiGroupPut, keyRequest?: unknown) {
  return await apiFetch<ApiGroup>({
    url: ApiGroupEndpoint,
    post: params,
    method: API_FETCH_OPT_METHODS.put,
    keyRequest,
  });
}
export async function apiPostGroupFetch(params: ApiGroupPost, keyRequest?: unknown) {
  return await apiFetch<ApiGroup>({
    url: ApiGroupEndpoint,
    post: params,
    keyRequest,
  });
}
