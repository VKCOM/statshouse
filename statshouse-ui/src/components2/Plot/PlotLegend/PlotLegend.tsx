import React from 'react';
import cn from 'classnames';
import css from './style.module.css';

export type PlotLegendProps = {
  short?: boolean;
};
export function PlotLegend({ short }: PlotLegendProps) {
  return <div className={cn(css.plotLegend, short && css.plotLegendShort)}>legend</div>;
}
