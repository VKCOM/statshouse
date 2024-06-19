import React, { useCallback, useMemo } from 'react';
import uPlot from 'uplot';
import { type PlotViewProps } from './PlotView';
import css from './style.module.css';
import { UPlotWrapper } from 'components';
import { dataIdxNearest } from 'common/dataIdxNearest';
import { black, grey, greyDark } from '../../../view/palette';
import { font, getYAxisSize, xAxisValues, xAxisValuesCompact } from '../../../common/axisValues';
import { formatByMetricType, incrs, splitByMetricType } from '../../../common/formatByMetricType';
import { METRIC_TYPE } from '../../../api/enum';
import { calcYRange } from '../../../common/calcYRange';
import { xRangeStatic } from './xRangeStatic';
import { dateRangeFormat } from './dateRangeFormat';
import { useStateToRef } from '../../../hooks';
import cn from 'classnames';
import { useStatsHouseShallow } from '../../../store2';

const themeDark = false;
const xAxisSize = 16;
const unFocusAlfa = 1;
const yLockDefault = { min: 0, max: 0 };
const compact = false;

export function PlotViewMetric({ className, plotKey }: PlotViewProps) {
  const { yLock, plotData } = useStatsHouseShallow(({ plotsData, params: { plots } }) => ({
    plotData: plotsData[plotKey],
    yLock: plots[plotKey]?.yLock,
  }));
  const yLockRef = useStateToRef(yLock ?? yLockDefault);
  const getAxisStroke = useCallback(() => (themeDark ? grey : black), []);
  // const metricType = useMemo(() => {
  //   if (plot.metricUnit != null) {
  //     return plot.metricUnit;
  //   }
  //   return METRIC_TYPE.none;
  //   // return getMetricType(plot.what, meta?.metric_type);
  // }, [plot.metricUnit]);
  const opts = useMemo<Partial<uPlot.Options>>(() => {
    const grid: uPlot.Axis.Grid = {
      stroke: themeDark ? greyDark : grey,
      width: 1 / devicePixelRatio,
    };
    const opt: Partial<uPlot.Options> = {
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
      focus: {
        alpha: unFocusAlfa, // avoid redrawing unfocused series
      },
      axes: [
        {
          grid: grid,
          ticks: grid,
          values: compact ? xAxisValuesCompact : xAxisValues,
          font: font,
          size: xAxisSize,
          stroke: getAxisStroke,
        },
        {
          grid: grid,
          ticks: grid,
          values: (_, splits) => splits.map(formatByMetricType(plotData?.metricUnit ?? METRIC_TYPE.none)),
          size: getYAxisSize(16),
          font: font,
          stroke: getAxisStroke,
          splits:
            plotData?.metricUnit === METRIC_TYPE.none
              ? undefined
              : splitByMetricType(plotData?.metricUnit ?? METRIC_TYPE.none),
          incrs,
        },
      ],
      scales: {
        x: { auto: false, range: xRangeStatic },
        y: {
          auto: (u) => !yLockRef.current || (yLockRef.current.min === 0 && yLockRef.current.max === 0),
          range: (u: uPlot): uPlot.Range.MinMax => {
            const min = yLockRef.current.min;
            const max = yLockRef.current.max;
            if (min !== 0 || max !== 0) {
              return [min, max];
            }
            return calcYRange(u, true);
          },
        },
      },
      series: [
        {
          value: dateRangeFormat, //'{DD}/{MM}/{YY} {H}:{mm}:{ss}',
        },
      ],
      legend: {
        show: false,
        live: true, //!compact,
        markers: {
          width: devicePixelRatio > 1 ? 1.5 : 1,
        },
      },
    };
    return opt;
  }, [getAxisStroke, plotData?.metricUnit, yLockRef]);
  return (
    <div className={cn(css.plotViewMetric, className)}>
      {!!plotData && (
        <UPlotWrapper
          opts={opts}
          className={css.plotViewMetricInner}
          data={plotData.dataView}
          series={plotData.series}
          bands={plotData.bands}
          scales={plotData.scales}
        ></UPlotWrapper>
      )}
    </div>
  );
}
