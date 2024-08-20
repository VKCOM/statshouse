import { PlotParams } from 'url2';

export function changePlotParamForData(plot?: PlotParams, prevPlot?: PlotParams) {
  return (
    plot == null ||
    prevPlot == null ||
    plot.filterIn !== prevPlot.filterIn ||
    plot.filterNotIn !== prevPlot.filterNotIn ||
    plot.groupBy !== prevPlot.groupBy ||
    plot.numSeries !== prevPlot.numSeries ||
    plot.what !== prevPlot.what ||
    plot.promQL !== prevPlot.promQL ||
    plot.customAgg !== prevPlot.customAgg ||
    plot.maxHost !== prevPlot.maxHost ||
    plot.useV2 !== prevPlot.useV2 ||
    plot.type !== prevPlot.type
  );
  //       plot.filterIn !== dataPlot.filterIn ||
  //       plot.filterNotIn !== dataPlot.filterNotIn ||
  //       plot.groupBy !== dataPlot.groupBy ||
  //       plot.numSeries !== dataPlot.numSeries ||
  //       plot.what !== dataPlot.what ||
  //       plot.promQL !== dataPlot.promQL ||
  //       plot.customAgg !== dataPlot.customAgg ||
  //       plot.maxHost !== dataPlot.maxHost ||
  //       plot.useV2 !== dataPlot.useV2 ||
  //       plot.type !== dataPlot.type
}
