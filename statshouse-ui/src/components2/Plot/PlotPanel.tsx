import React from 'react';
import css from './style.module.css';
import cn from 'classnames';
export type PlotPanelProps = {
  children?: React.ReactNode;
  className?: string;
};
export function PlotPanel({ children, className }: PlotPanelProps) {
  return (
    <div className={css.plotPanelWrap}>
      {/*<div className={css.plotPanelLayout}>*/}
      <div className={cn(css.plotPanel, className)}>{children}</div>
      {/*</div>*/}
    </div>
  );
}
