import { useMemo } from 'react';
import { PlotParams, toPlotKey } from '../url/queryParams';
import { replaceVariable } from '../view/utils';
import { queryURLCSV } from '../view/api';
import { useStore } from '../store';
import { shallow } from 'zustand/shallow';

export function useLinkCSV(indexPlot: number) {
  const { params, width, timeRange } = useStore(
    ({ params, uPlotsWidth, timeRange }) => ({
      params,
      width: uPlotsWidth[indexPlot],
      timeRange,
    }),
    shallow
  );
  return useMemo(() => {
    const { plots, variables, timeShifts } = params;
    const plotKey = toPlotKey(indexPlot, '0');
    const lastPlotParams: PlotParams | undefined = replaceVariable(plotKey, plots[indexPlot], variables);
    const agg =
      lastPlotParams.customAgg === -1
        ? `${Math.floor(width / 2)}`
        : lastPlotParams.customAgg === 0
        ? `${Math.floor(width * devicePixelRatio)}`
        : `${lastPlotParams.customAgg}s`;
    return queryURLCSV(lastPlotParams, timeRange, timeShifts, agg, params);
  }, [indexPlot, params, timeRange, width]);
}
