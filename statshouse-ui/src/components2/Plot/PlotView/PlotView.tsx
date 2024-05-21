import React from 'react';
import { PlotInfo, PlotParams } from 'store2';
import { PLOT_TYPE } from '../../../api/enum';
import { PlotViewMetric } from './PlotViewMetric';
import { PlotViewEvent } from './PlotViewEvent';
export type PlotViewProps = {
  plot?: PlotParams;
  plotInfo?: PlotInfo;
};
export function PlotView({ plot, plotInfo }: PlotViewProps) {
  if (!plot || !plotInfo) {
    return null;
  }
  switch (plot.type) {
    case PLOT_TYPE.Metric:
      return <PlotViewMetric plot={plot} plotInfo={plotInfo}></PlotViewMetric>;
    case PLOT_TYPE.Event:
      return <PlotViewEvent plot={plot} plotInfo={plotInfo}></PlotViewEvent>;
    default:
      return null;
  }
}
