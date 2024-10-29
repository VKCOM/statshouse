import { QueryParams } from '../queryParams';
import { getDefaultParams } from '../getDefault';
import { metricEncode } from './metric';
import { GET_PARAMS, PLOT_TYPE } from '../../api/enum';
import { orderPlotSplitter, removeValueChar } from '../constants';
import { dequal } from 'dequal/lite';

export function widgetsParamsEncode(
  params: QueryParams,
  defaultParams: QueryParams = getDefaultParams()
): [string, string][] {
  const paramArr: [string, string][] = [];
  if (defaultParams.plots !== params.plots) {
    Object.values(params.plots).forEach((p) => {
      if (p) {
        switch (p.type) {
          case PLOT_TYPE.Metric:
          case PLOT_TYPE.Event:
            paramArr.push(...metricEncode(p, defaultParams.plots[p.id]));
            break;
          default:
        }
      }
    });
    //remove plots
    Object.values(defaultParams.plots).forEach((dPlot) => {
      if (dPlot && !params.plots[dPlot.id]) {
        paramArr.push([GET_PARAMS.plotPrefix + dPlot.id, removeValueChar]);
      }
    });
  }
  if (!dequal(defaultParams.orderPlot, params.orderPlot)) {
    paramArr.push([GET_PARAMS.orderPlot, params.orderPlot.join(orderPlotSplitter)]);
  }
  return paramArr;
}
