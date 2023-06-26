// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { apiFetch } from './api';

const ApiDashboardListEndpoint = '/api/dashboards-list';

/**
 * Response endpoint api/dashboards-list
 */
export type ApiDashboardList = {
  data: GetDashboardListResp;
};

export type GetDashboardListResp = {
  dashboards: DashboardShortInfo[] | null;
};

export type DashboardShortInfo = {
  id: number;
  name: string;
  description: string;
};

export async function apiDashboardListFetch(keyRequest?: unknown) {
  return await apiFetch<ApiDashboardList>({ url: ApiDashboardListEndpoint, keyRequest });
}
