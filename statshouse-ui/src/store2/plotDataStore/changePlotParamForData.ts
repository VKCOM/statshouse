import type { PlotParams } from '@/url2';
import { dequal } from 'dequal/lite';

export function changePlotParamForData(plot?: PlotParams, prevPlot?: PlotParams) {
  return (
    plot == null ||
    prevPlot == null ||
    !dequal(plot.filterIn, prevPlot.filterIn) ||
    !dequal(plot.filterNotIn, prevPlot.filterNotIn) ||
    !dequal(plot.groupBy, prevPlot.groupBy) ||
    plot.numSeries !== prevPlot.numSeries ||
    !dequal(plot.what, prevPlot.what) ||
    plot.promQL !== prevPlot.promQL ||
    plot.customAgg !== prevPlot.customAgg ||
    plot.maxHost !== prevPlot.maxHost ||
    plot.backendVersion !== prevPlot.backendVersion ||
    plot.type !== prevPlot.type ||
    plot.prometheusCompat !== prevPlot.prometheusCompat ||
    plot.totalLine !== prevPlot.totalLine ||
    plot.filledGraph !== prevPlot.filledGraph ||
    plot.logScale !== prevPlot.logScale
  );
}
