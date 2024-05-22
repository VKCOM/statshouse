import React from 'react';
import { type PlotInfo } from 'store2';
import { MetricName } from 'components2';

import css from './style.module.css';
import { Link } from 'react-router-dom';

export type DashboardPlotProps = {
  children?: React.ReactNode;
  plotInfo?: PlotInfo;
};

export function DashboardPlot({ children, plotInfo }: DashboardPlotProps) {
  if (!plotInfo) {
    return null;
  }
  return (
    <div className={css.dashboardPlot}>
      <div className={css.dashboardPlotHeader}>
        <Link to={{ pathname: '/2/view', search: plotInfo.link }} className={css.dashboardPlotHeaderLink}>
          <MetricName
            metricName={plotInfo?.metricName}
            metricWhat={plotInfo?.metricWhat}
            className={'flex-grow-1 w-0'}
          ></MetricName>
        </Link>
      </div>
      <div>{children}</div>
    </div>
  );
}
