// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { arrToObj, type QueryParams, toTreeObj, urlDecode } from 'url2';
import { isArray, isObject } from 'common/helpers';

export function isUrlSearchArray(item: unknown): item is [string, string][] {
  return isArray(item) && item.every((v) => isArray(v) && typeof v[0] === 'string' && typeof v[1] === 'string');
}

export function normalizeDashboard(data: unknown, defaultParams: QueryParams): QueryParams {
  if (isObject(data) && isUrlSearchArray(data.searchParams)) {
    const dashboardParam = urlDecode(toTreeObj(arrToObj(data.searchParams)), defaultParams);
    //fix save page
    dashboardParam.tabNum = '-1';

    return dashboardParam;
  }
  return defaultParams;
}
