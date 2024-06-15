import type { Location } from 'history';

import { createStore } from '../createStore';
import { appHistory } from '../../common/appHistory';
import { type PlotKey, type PlotParams, type QueryParams, type TimeRange } from './queryParams';
import {
  addPlot,
  arrToObj,
  defaultBaseRange,
  getDefaultParams,
  getHomePlot,
  ProduceUpdate,
  readTimeRange,
  timeRangeAbbrevExpand,
  toTreeObj,
  TreeParamsObject,
  treeParamsObjectValueSymbol,
  updateParams,
  updatePlot,
  updateTimeRange,
} from './lib';
import { urlDecode } from './urlDecode';
import { apiDashboardFetch } from '../../api/dashboard';
import { GET_PARAMS, TIME_RANGE_KEYS_TO } from '../../api/enum';
import { produce } from 'immer';
import { isArray, isObject, mergeLeft } from '../../common/helpers';
import { debug } from '../../common/debug';
import { urlEncode } from './urlEncode';
import { globalSettings } from '../../common/settings';

export type UrlStore = {
  params: QueryParams;
  saveParams: QueryParams;
};

let lastSearch: string = appHistory.location.search;

const isValid: string[] = ['/2/view', '/2/embed'];
export function validPath(location: Location) {
  return isValid.indexOf(location.pathname) > -1;
}

export const useUrlStore = createStore<UrlStore>((setState, getState) => {
  let prevLocation = appHistory.location;
  appHistory.listen(({ location }) => {
    if (prevLocation.search !== location.search || prevLocation.pathname !== location.pathname) {
      prevLocation = location;
      if (validPath(prevLocation) && lastSearch !== prevLocation.search) {
        lastSearch = prevLocation.search;
        getUrlState(getState().saveParams, prevLocation).then((res) => {
          setState((s) => mergeLeft(s, { ...res }));
        });
      }
    }
  });
  const saveParams = getDefaultParams();
  lastSearch = prevLocation.search;
  getUrlState(saveParams, prevLocation).then((res) => {
    setState((s) => mergeLeft(s, { ...res }));
  });
  return {
    params: saveParams,
    saveParams: saveParams,
  };
}, 'useUrlStore');

export async function getUrlState(
  prevParam: QueryParams,
  location: Location
): Promise<Pick<UrlStore, 'params' | 'saveParams'>> {
  const urlSearchArray = [...new URLSearchParams(location.search)];
  const urlObject = arrToObj(urlSearchArray);
  const urlTree = toTreeObj(urlObject);
  const saveParams = await loadDashboard(prevParam, urlTree, getDefaultParams());
  const params = urlDecode(urlTree, saveParams);
  resetDefaultParams(params);
  return {
    params,
    saveParams,
  };
}

export function setUrlStore(next: ProduceUpdate<UrlStore>, replace: boolean = false) {
  const nextState = produce<UrlStore>(useUrlStore.getState(), next);
  const search = getUrl(nextState);
  if (replace) {
    appHistory.replace({ search });
  } else {
    appHistory.push({ search });
  }
}

export function getUrl(state: UrlStore): string {
  const urlSearchArray = urlEncode(state.params, state.saveParams);
  return new URLSearchParams(urlSearchArray).toString();
}

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

export function normalizeDashboard(data: unknown, defaultParams: QueryParams): QueryParams {
  if (isObject(data) && isUrlSearchArray(data.searchParams)) {
    return urlDecode(toTreeObj(arrToObj(data.searchParams)), defaultParams);
  }
  return defaultParams;
}

export function isUrlSearchArray(item: unknown): item is [string, string][] {
  return isArray(item) && item.every((v) => isArray(v) && typeof v[0] === 'string' && typeof v[1] === 'string');
}

export function resetDefaultParams(params: QueryParams) {
  let reset = false;
  if (Object.keys(params.plots).length === 0) {
    params = addPlot(getHomePlot(), params);
    reset = true;
  }
  params = produce(params, (p) => {
    if (+p.tabNum >= 0 && !p.plots[params.tabNum]) {
      p.tabNum = p.orderPlot.length > 1 ? '-1' : '0';
      reset = true;
    }

    if (globalSettings.disabled_v1) {
      Object.values(p.plots).forEach((plot) => {
        if (plot) {
          plot.useV2 = true;
          reset = true;
        }
      });
    }
    if (!p.timeRange.from && p.timeRange.urlTo === TIME_RANGE_KEYS_TO.default) {
      const from = timeRangeAbbrevExpand(defaultBaseRange);
      p.timeRange = readTimeRange(from, TIME_RANGE_KEYS_TO.default);
      reset = true;
    } else if (p.timeRange.urlTo === TIME_RANGE_KEYS_TO.default) {
      p.timeRange.urlTo = p.timeRange.to = p.timeRange.now;
      reset = true;
    } else if (p.timeRange.from >= 0) {
      p.timeRange.from = timeRangeAbbrevExpand(defaultBaseRange);
      reset = true;
    }
  });
  if (reset) {
    setUrlStore((p) => {
      p.params = params;
    }, true);
  }
}

export function setParams(next: ProduceUpdate<QueryParams>) {
  setUrlStore(updateParams(next));
}

export function setTimeRange(next: ProduceUpdate<TimeRange>, replace?: boolean) {
  setUrlStore(updateTimeRange(next), replace);
}

export function setPlot(plotKey: PlotKey, next: ProduceUpdate<PlotParams>) {
  setUrlStore(updatePlot(plotKey, next));
}
