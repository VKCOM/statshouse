import { isPlotKey, TreeParamsObject, treeParamsObjectValueSymbol } from '../urlHelpers';
import { PlotKey, PlotParams, QueryParams } from '../queryParams';
import { uniqueArray } from '../../common/helpers';
import { GET_PARAMS, PLOT_TYPE, toPlotType } from '../../api/enum';
import { orderPlotSplitter } from '../constants';
import { metricDecode } from './metric';

export function widgetsParamsDecode(
  searchParams: TreeParamsObject,
  keys: PlotKey[],
  defaultParams: QueryParams
): Pick<QueryParams, 'plots' | 'orderPlot'> {
  const orderPlot = uniqueArray([
    ...(searchParams[GET_PARAMS.orderPlot]?.[treeParamsObjectValueSymbol]?.[0]
      ?.split(orderPlotSplitter)
      .filter((s) => isPlotKey(s)) ?? defaultParams.orderPlot),
    ...keys,
  ]);
  const plots: Partial<Record<PlotKey, PlotParams>> = {};
  keys.forEach((key) => {
    const plotSearchParams = searchParams[GET_PARAMS.plotPrefix + key] ?? (key === '0' ? searchParams : undefined);
    const type =
      toPlotType(plotSearchParams?.[GET_PARAMS.metricType]?.[treeParamsObjectValueSymbol]?.[0]) ??
      defaultParams.plots[key]?.type;
    let p: PlotParams | undefined;
    switch (type) {
      case PLOT_TYPE.Metric:
      case PLOT_TYPE.Event:
      default:
        p = metricDecode(key, plotSearchParams, defaultParams.plots[key]);
        break;
    }
    if (p) {
      plots[key] = p;
    } else {
      const remove = orderPlot.indexOf(key);
      if (remove > -1) {
        orderPlot.splice(remove, 1);
      }
    }
  });
  //fix event link
  orderPlot.forEach((pK) => {
    if (plots[pK]) {
      plots[pK]!.events = plots[pK]!.events.filter((eK) => !!plots[eK]);
    }
  });
  return { plots, orderPlot };
}
