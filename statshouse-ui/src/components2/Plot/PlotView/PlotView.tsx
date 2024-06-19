import React from 'react';
import { PLOT_TYPE } from 'api/enum';
import { PlotViewMetric } from './PlotViewMetric';
import { PlotViewEvent } from './PlotViewEvent';
import { PlotKey } from 'url2';

export type PlotViewProps = {
  className?: string;
  plotKey: PlotKey;
};
export function PlotView({ className, plotKey }: PlotViewProps) {
  // if (!plot || !plotInfo || !plotData) {
  //   return null;
  // }
  switch (plotKey) {
    case PLOT_TYPE.Metric:
      return <PlotViewMetric className={className} plotKey={plotKey}></PlotViewMetric>;
    case PLOT_TYPE.Event:
      return <PlotViewEvent className={className} plotKey={plotKey}></PlotViewEvent>;
    default:
      return null;
  }
}
