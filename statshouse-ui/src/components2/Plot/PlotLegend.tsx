import React from 'react';

import css from './style.module.css';
import cn from 'classnames';

export type PlotLegendProps = {
  short?: boolean;
};
export function PlotLegend({ short }: PlotLegendProps) {
  return <div className={cn(css.plotLegend, short && css.plotLegendShort)}></div>;
}
