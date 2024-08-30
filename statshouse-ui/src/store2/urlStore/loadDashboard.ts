// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { getDefaultParams, type QueryParams, type TreeParamsObject, treeParamsObjectValueSymbol } from 'url2';
import { GET_PARAMS } from 'api/enum';
import { debug } from 'common/debug';
import { apiDashboardFetch, ApiDashboardGet } from 'api/dashboard';
import { readDataDashboard } from './readDataDashboard';

export function getDashboardId(urlTree: TreeParamsObject) {
  return urlTree[GET_PARAMS.dashboardID]?.[treeParamsObjectValueSymbol]?.[0];
}
export function getDashboardVersion(urlTree: TreeParamsObject) {
  return urlTree[GET_PARAMS.dashboardVersion]?.[treeParamsObjectValueSymbol]?.[0];
}

export async function loadDashboard(
  prevParam: QueryParams,
  urlTree: TreeParamsObject,
  defaultParams = getDefaultParams()
): Promise<{ params: QueryParams; error?: Error }> {
  const dashboardId = getDashboardId(urlTree);
  const dashboardVersion = getDashboardVersion(urlTree);

  let dashboardParams = defaultParams;
  if (dashboardId) {
    if (dashboardId && prevParam.dashboardId === dashboardId) {
      return { params: prevParam };
    }
    const urlGetParams: ApiDashboardGet = { [GET_PARAMS.dashboardID]: dashboardId };
    if (dashboardVersion != null) {
      urlGetParams[GET_PARAMS.dashboardApiVersion] = dashboardVersion;
    }

    const { response, error } = await apiDashboardFetch(urlGetParams);
    if (error) {
      debug.error(error);
      return { params: dashboardParams, error };
    }
    if (response) {
      dashboardParams = readDataDashboard(response.data, defaultParams);
    }
  }
  return { params: dashboardParams };
}
