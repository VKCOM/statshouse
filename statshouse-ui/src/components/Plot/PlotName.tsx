import React, { useMemo } from 'react';
import cn from 'classnames';
import { PlotParams } from '../../url/queryParams';
import { PlotStore } from '../../store';
import { promQLMetric } from '../../view/promQLMetric';
import { whatToWhatDesc } from '../../view/whatToWhatDesc';

export type PlotNameProps = {
  plot: PlotParams;
  plotData: PlotStore;
  className?: string;
};
export function PlotName({ plot, plotData, className }: PlotNameProps) {
  const metricName = useMemo(
    () => (plot.metricName !== promQLMetric ? plot.metricName : plotData.nameMetric),
    [plot.metricName, plotData.nameMetric]
  );
  const what = useMemo(
    () =>
      plot.metricName === promQLMetric
        ? plotData.whats.map((qw) => whatToWhatDesc(qw)).join(', ')
        : plot.what.map((qw) => whatToWhatDesc(qw)).join(', '),
    [plot.metricName, plot.what, plotData.whats]
  );

  if (plot.customName) {
    return <span className={cn(className, 'text-body text-truncate')}>{plot.customName}</span>;
  }
  if (metricName) {
    return (
      <span className={cn(className)}>
        <span className="text-body text-truncate">{metricName}</span>
        {!!what && <span className="text-secondary text-truncate">:&nbsp;{what}</span>}
      </span>
    );
  }
  return <span className={cn(className)}>&nbsp;</span>;
}
