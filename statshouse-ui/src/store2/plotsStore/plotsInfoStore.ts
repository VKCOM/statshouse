import { createStore } from '../createStore';
import {
  getAddPlotLink,
  getGroupPlotsMap,
  getPlotLink,
  GroupPlotsMap,
  PlotKey,
  QueryParams,
  UrlStore,
  useUrlStore,
} from '../urlStore';
import { isNotNil, mergeLeft } from '../../common/helpers';
import { To } from 'react-router-dom';

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
} & GroupPlotsMap;

export const usePlotsInfoStore = createStore<PlotsInfoStore>(
  () => ({
    ...updatePlots(useUrlStore.getState().params),
  }),
  'usePlotsStore'
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
    plotsInfo: mergeLeft(
      params.plots,
      Object.fromEntries(
        Object.entries(params.plots)
          .map(([plotKey, plot]) => {
            if (!plot) {
              return undefined;
            }
            const metricWhat = plot.customName ? '' : plot.what.join(', ');
            const metricName = plot.customName || plot.metricName || '';
            const link = getPlotLink(plot.id, params, saveParams);
            return [plotKey, { metricName, metricWhat, link }];
          })
          .filter(isNotNil)
      )
    ),
    // orderPlot: [...params.orderPlot],
    dashboardLink: { pathname: '/2/view', search: getPlotLink('-1', params, saveParams) },
    dashboardSettingLink: { pathname: '/2/view', search: getPlotLink('-2', params, saveParams) },
    addLink: { pathname: '/2/view', search: getAddPlotLink(params, saveParams) },
    tabNum: params.tabNum,
    ...getGroupPlotsMap(params),
  };
}

useUrlStore.subscribe(plotsStoreSubscribe);
