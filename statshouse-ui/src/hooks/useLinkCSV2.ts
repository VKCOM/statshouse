import { useMemo } from 'react';
import { type PlotKey, type QueryParams } from 'url2';
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
