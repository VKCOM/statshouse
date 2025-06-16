import type { PlotKey, QueryParams } from '@/url2';
import { ProduceUpdate } from '@/store2/helpers';

export function updatePlotRemove(plotKey: PlotKey): ProduceUpdate<QueryParams> {
  return (params) => {
    delete params.plots[plotKey];
    params.orderVariables.forEach((variableKey) => {
      if (params.variables[variableKey]) {
        const link = params.variables[variableKey].link.filter(([pKey]) => pKey !== plotKey);
        if (link.length !== params.variables[variableKey].link.length) {
          params.variables[variableKey].link = link;
        }
      }
    });
  };
}
