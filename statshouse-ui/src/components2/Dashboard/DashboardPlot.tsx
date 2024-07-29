import React from 'react';

import css from './style.module.css';

export type DashboardPlotProps = {
  children?: React.ReactNode;
};

export function DashboardPlot({ children }: DashboardPlotProps) {
  return (
    <div className={css.dashboardPlot}>
      <div>{children}</div>
    </div>
  );
}
