import React from 'react';
import { PlotParams } from 'store2';
import css from './style.module.css';

export type PlotControlProps = {
  plot?: PlotParams;
};
export function PlotControl({ plot }: PlotControlProps) {
  return <div className={css.plotControl}>{plot?.metricName ?? ''} control</div>;
}
