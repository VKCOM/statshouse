import React, { useMemo } from 'react';
import uPlot from 'uplot';
import { type PlotViewProps } from './PlotView';
import css from './style.module.css';
import { UPlotWrapper } from 'components';
import { dataIdxNearest } from 'common/dataIdxNearest';

export function PlotViewMetric({ plot, plotInfo }: Required<PlotViewProps>) {
  const opts = useMemo<Partial<uPlot.Options>>(
    () => ({
      cursor: {
        lock: true,
        drag: {
          dist: 5, // try to prevent double-click-selections a bit
          x: true,
          y: true,
          uni: Infinity,
        },
        focus: {
          prox: Infinity, // always have one series focused
        },
        sync: {
          key: '1',
          // filters: {
          //   sub(event, client, x, y, w, h, i) {
          //     return true;
          // return event !== 'mouseup' && event !== 'mousedown';
          // },
          // pub(event, client, x, y, w, h, i) {
          // console.log({ event, client, x, y, w, h, i });
          // return true;
          // return event !== 'mousemove';
          // return event !== 'mouseup' && event !== 'mousedown';
          // },
          // },
        },
        dataIdx: dataIdxNearest,
      },
      legend: {
        show: false,
        live: true, //!compact,
        markers: {
          width: devicePixelRatio > 1 ? 1.5 : 1,
        },
      },
    }),
    []
  );
  return (
    <div className={css.plotViewMetric}>
      <UPlotWrapper opts={opts} className={css.plotViewMetricInner}></UPlotWrapper>
    </div>
  );
}
