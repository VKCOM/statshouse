// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useMemo } from 'react';
import type { PlotKey, QueryParams } from '@/url2';
import { useStatsHouse } from '../store2';
import { GET_PARAMS } from '../api/enum';
import { getLoadPlotUrlParams } from '../store2/plotDataStore/loadPlotData';
import { toFlatPairs, toString } from '../common/helpers';

export function queryURLCSV(plotKey: PlotKey, params: QueryParams): string {
  const urlParams = getLoadPlotUrlParams(plotKey, params);
  if (!urlParams) {
    return '';
  }
  urlParams[GET_PARAMS.metricDownloadFile] = 'csv';

  const strParams = new URLSearchParams(toFlatPairs(urlParams, toString)).toString();
  return `/api/query?${strParams}`;
}

export function useLinkCSV2(plotKey: PlotKey) {
  const params = useStatsHouse(({ params }) => params);
  return useMemo(() => queryURLCSV(plotKey, params), [params, plotKey]);
}
