// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { getDefaultParams, type QueryParams, type TreeParamsObject, treeParamsObjectValueSymbol } from '@/url2';
import { GET_PARAMS } from '@/api/enum';
import { debug } from '@/common/debug';
import { apiDashboard } from '@/api/dashboard';
import { readDataDashboard } from './readDataDashboard';
import { ExtendedError } from '@/api/api';
import { useErrorStore } from '@/store2/errors';

export function getDashboardId(urlTree: TreeParamsObject) {
  return urlTree[GET_PARAMS.dashboardID]?.[treeParamsObjectValueSymbol]?.[0];
}
export function getDashboardVersion(urlTree: TreeParamsObject) {
  return urlTree[GET_PARAMS.dashboardVersion]?.[treeParamsObjectValueSymbol]?.[0];
}

export async function loadDashboard(
  urlTree: TreeParamsObject,
  defaultParams = getDefaultParams()
): Promise<{ params: QueryParams; error?: ExtendedError }> {
  const dashboardId = getDashboardId(urlTree);
  const dashboardVersion = getDashboardVersion(urlTree);

  let dashboardParams = defaultParams;
  if (dashboardId) {
    const { response, error } = await apiDashboard(dashboardId, dashboardVersion);
    if (error) {
      debug.error(error);
      return { params: dashboardParams, error };
    }
    if (response) {
      dashboardParams = readDataDashboard(response.data, defaultParams);
      if (!dashboardParams.dashboardId || !dashboardParams.dashboardVersion) {
        useErrorStore.getState().addError(new Error('The dashboard does not exist. Displayed the default dashboards'));
      }
    }
  }
  return { params: dashboardParams };
}
