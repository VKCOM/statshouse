import React from 'react';
import { PLOT_TYPE } from 'api/enum';
import { PlotViewMetric } from './PlotViewMetric';
import { PlotViewEvent } from './PlotViewEvent';
import { PlotKey } from 'url2';
import { useStatsHouse } from 'store2';

export type PlotViewProps = {
  className?: string;
  plotKey: PlotKey;
};
export function PlotView(props: PlotViewProps) {
  const type = useStatsHouse((s) => s.params.plots[props.plotKey]?.type ?? PLOT_TYPE.Metric);
  switch (type) {
    case PLOT_TYPE.Metric:
      return <PlotViewMetric {...props}></PlotViewMetric>;
    case PLOT_TYPE.Event:
      return <PlotViewEvent {...props}></PlotViewEvent>;
    default:
      return null;
  }
}
