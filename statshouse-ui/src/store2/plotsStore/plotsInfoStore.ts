import { createStore } from '../createStore';
import {
  defaultBaseRange,
  getAddPlotLink,
  getGroupPlotsMap,
  getPlotLink,
  GroupPlotsMap,
  isPromQL,
  PlotKey,
  QueryParams,
  UrlStore,
  useUrlStore,
} from '../urlStore';
import { isNotNil } from '../../common/helpers';
import { To } from 'react-router-dom';
import { TimeRangeAbbrev } from '../../api/enum';

export type PlotInfo = {
  metricName: string;
  metricWhat: string;
  link: string;
};
export type PlotsInfoStore = {
  plotsInfo: Partial<Record<PlotKey, PlotInfo>>;
  dashboardLink: To;
  dashboardSettingLink: To;
  addLink: To;
  tabNum: PlotKey;
  baseRange: TimeRangeAbbrev;
} & GroupPlotsMap;

export const usePlotsInfoStore = createStore<PlotsInfoStore>(
  () => ({
    ...updatePlots(useUrlStore.getState().params),
  }),
  'usePlotsInfoStore'
);

export function plotsStoreSubscribe(state: UrlStore, prevState: UrlStore) {
  usePlotsInfoStore.setState(() => {
    const nextState: Partial<PlotsInfoStore> = {};
    if (state.params.plots !== prevState.params.plots) {
      Object.assign(nextState, updatePlots(state.params, state.saveParams));
    } else {
      if (state.params.orderPlot !== prevState.params.orderPlot) {
        nextState.viewOrderPlot = [...state.params.orderPlot];
      }
      if (state.params.tabNum !== prevState.params.tabNum) {
        nextState.tabNum = state.params.tabNum;
        nextState.addLink = getAddPlotLink(state.params, state.saveParams);
      }
    }
    return nextState;
  });
}

export function updatePlots(params: QueryParams, saveParams?: QueryParams): PlotsInfoStore {
  return {
    plotsInfo: Object.fromEntries(
      Object.entries(params.plots)
        .map(([plotKey, plot]) => {
          if (!plot) {
            return undefined;
          }
          const isPlotPromQL = isPromQL(plot);
          const metricWhat = isPlotPromQL || plot.customName ? '' : plot.what.join(', ');
          const metricName = plot.customName || (!isPlotPromQL && plot.metricName) || `plot#${plotKey}`;
          const link = getPlotLink(plot.id, params, saveParams);
          return [plotKey, { metricName, metricWhat, link }];
        })
        .filter(isNotNil)
    ),
    // orderPlot: [...params.orderPlot],
    dashboardLink: { pathname: '/2/view', search: getPlotLink('-1', params, saveParams) },
    dashboardSettingLink: { pathname: '/2/view', search: getPlotLink('-2', params, saveParams) },
    addLink: { pathname: '/2/view', search: getAddPlotLink(params, saveParams) },
    tabNum: params.tabNum,
    baseRange: defaultBaseRange,
    ...getGroupPlotsMap(params),
  };
}

useUrlStore.subscribe(plotsStoreSubscribe);

export function setBaseRange(r: TimeRangeAbbrev) {
  usePlotsInfoStore.setState({ baseRange: r });
}
