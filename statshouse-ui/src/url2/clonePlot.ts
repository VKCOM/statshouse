import { PlotParams } from './queryParams';
import { deepClone } from '../common/helpers';

export function clonePlot(plot: PlotParams): PlotParams;
export function clonePlot(plot: undefined): undefined;
export function clonePlot(plot?: PlotParams): PlotParams | undefined {
  if (plot == null) {
    return plot;
  }
  return {
    id: plot.id,
    metricName: plot.metricName,
    customName: plot.customName,
    customDescription: plot.customDescription,
    promQL: plot.promQL,
    metricUnit: plot.metricUnit,
    what: [...plot.what],
    customAgg: plot.customAgg,
    groupBy: [...plot.groupBy],
    filterIn: deepClone(plot.filterIn),
    filterNotIn: deepClone(plot.filterNotIn),
    numSeries: plot.numSeries,
    backendVersion: plot.backendVersion,
    yLock: {
      min: plot.yLock.min,
      max: plot.yLock.max,
    },
    maxHost: plot.maxHost,
    type: plot.type,
    events: [...plot.events],
    eventsBy: [...plot.eventsBy],
    eventsHide: [...plot.eventsHide],
    totalLine: plot.totalLine,
    filledGraph: plot.filledGraph,
    timeShifts: [...plot.timeShifts],
    prometheusCompat: plot.prometheusCompat,
  };
}
