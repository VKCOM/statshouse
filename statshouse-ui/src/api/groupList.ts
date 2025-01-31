// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { apiFetch } from './api';
import { GET_PARAMS } from './enum';

const ApiGroupListEndpoint = '/api/group-list';

/**
 * Response endpoint api/group-list
 */
export type ApiGroupList = {
  data: GetGroupListResp;
};

export type GetGroupListResp = {
  groups: GroupShort[] | null;
};

export type GroupShort = {
  id: number;
  name: string;
  weight: number;
  disable?: boolean;
};

export async function apiGroupListFetch(keyRequest?: unknown) {
  return await apiFetch<ApiGroupList>({
    url: ApiGroupListEndpoint,
    get: { [GET_PARAMS.metricsListAndDisabled]: '1' },
    keyRequest,
  });
}
