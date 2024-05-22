import React from 'react';
import css from './style.module.css';
export type PlotPanelProps = {
  children?: React.ReactNode;
};
export function PlotPanel({ children }: PlotPanelProps) {
  return (
    <div className={css.plotPanelWrap}>
      {/*<div className={css.plotPanelLayout}>*/}
      <div className={css.plotPanel}>{children}</div>
      {/*</div>*/}
    </div>
  );
}
