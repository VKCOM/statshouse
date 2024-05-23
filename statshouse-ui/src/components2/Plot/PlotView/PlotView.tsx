import React from 'react';
import { PlotData, PlotInfo, PlotParams } from 'store2';
import { PLOT_TYPE } from '../../../api/enum';
import { PlotViewMetric } from './PlotViewMetric';
import { PlotViewEvent } from './PlotViewEvent';
export type PlotViewProps = {
  plot?: PlotParams;
  plotInfo?: PlotInfo;
  plotData?: PlotData;
};
export function PlotView({ plot, plotInfo, plotData }: PlotViewProps) {
  // if (!plot || !plotInfo || !plotData) {
  //   return null;
  // }
  switch (plot?.type) {
    case PLOT_TYPE.Metric:
      return <PlotViewMetric plot={plot} plotInfo={plotInfo} plotData={plotData}></PlotViewMetric>;
    case PLOT_TYPE.Event:
      return <PlotViewEvent plot={plot} plotInfo={plotInfo} plotData={plotData}></PlotViewEvent>;
    default:
      return null;
  }
}
