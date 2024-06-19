// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { getDefaultParams, type QueryParams, type TreeParamsObject, treeParamsObjectValueSymbol } from 'url2';
import { GET_PARAMS } from 'api/enum';
import { debug } from 'common/debug';
import { apiDashboardFetch } from 'api/dashboard';
import { normalizeDashboard } from './normalizeDashboard';

export function getDashboardId(urlTree: TreeParamsObject) {
  return urlTree[GET_PARAMS.dashboardID]?.[treeParamsObjectValueSymbol]?.[0];
}

export async function loadDashboard(
  prevParam: QueryParams,
  urlTree: TreeParamsObject,
  defaultParams = getDefaultParams()
) {
  const dashboardId = getDashboardId(urlTree);

  let dashboardParams = defaultParams;
  if (dashboardId) {
    if (dashboardId && prevParam.dashboardId === dashboardId) {
      return prevParam;
    }
    const { response, error } = await apiDashboardFetch({ [GET_PARAMS.dashboardID]: dashboardId });
    if (error) {
      debug.error(error);
    }
    if (response) {
      dashboardParams = normalizeDashboard(response.data?.dashboard?.data, {
        ...defaultParams,
        dashboardId: response.data.dashboard.dashboard_id.toString(),
        dashboardName: response.data.dashboard.name,
        dashboardDescription: response.data.dashboard.description,
        dashboardVersion: response.data.dashboard.version,
      });
    }
  }
  return dashboardParams;
}
