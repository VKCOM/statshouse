// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { PlotWidgetRouterProps } from '../PlotWidgetRouter';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { type StatsHouseStore, useStatsHouseShallow } from '@/store2';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useIntersectionObserver, useStateToRef, useUPlotPluginHooks } from '@/hooks';
import { useThemeStore } from '@/store2/themeStore';
import { black, grey, greyDark } from '@/view/palette';
import type uPlot from 'uplot';
import {
  LegendItem,
  UPlotPluginPortal,
  UPlotWrapper,
  UPlotWrapperPropsOpts,
  UPlotWrapperPropsScales,
} from '@/components/UPlotWrapper';
import type { PlotData, PlotValues } from '@/store2/plotDataStore';
import { setLiveMode } from '@/store2/liveModeStore';
import { formatByMetricType, getMetricType } from '@/common/formatByMetricType';
import { dataIdxNearest } from '@/common/dataIdxNearest';
import { font, getYAxisSize, xAxisValues, xAxisValuesCompact } from '@/common/axisValues';
import { bwd, fwd, log2Filter, log2Splits } from '@/common/helpers';
import { yAxisSize } from '@/common/settings';
import { metricTypeIncrs } from '@/components2/Plot/PlotView/constants';
import { xRangeStatic } from '@/components2/Plot/PlotView/xRangeStatic';
import { calcYRange } from '@/common/calcYRange';
import { dateRangeFormat } from '@/components2/Plot/PlotView/dateRangeFormat';
import { createPlotPreview } from '@/store2/plotPreviewStore';
import { setPlotVisibility, usePlotVisibilityStore } from '@/store2/plotVisibilityStore';
import cn from 'classnames';
import { PlotHealsStatus } from '@/components2/Plot/PlotView/PlotHealsStatus';
import { PlotHeader } from '@/components2/Plot/PlotView/PlotHeader';
import { PlotSubMenu } from '@/components2/Plot/PlotView/PlotSubMenu';
import css from '@/components2/Plot/PlotView/style.module.css';
import { PlotEventOverlay } from '@/components2/Plot/PlotView/PlotEventOverlay';
import { PlotLegend } from '@/components2';
import { rightPad, syncGroup, unFocusAlfa, yLockDefault } from '@/components2/PlotWidgets/MetricWidget/constant';
import { useMetricName } from '@/hooks/useMetricName';
import { useMetricMeta } from '@/hooks/useMetricMeta';
import { useMetricData } from '@/hooks/useMetricData';
import { produce } from 'immer';
import { resetZoom, setPlotYLock, setTimeRange } from '@/store2/methods';
import { PlotBox } from '@/components2/Plot/PlotView/PlotBox';

const selectorStore = ({
  params: {
    timeRange: { to, from },
  },
}: StatsHouseStore) => ({ timeRangeTo: to, timeRangeFrom: from });

export function MetricWidget({ className, isDashboard, isEmbed, fixRatio }: PlotWidgetRouterProps) {
  const {
    plot: { id, what: plotWhat, yLock, numSeries, metricUnit, logScale, events },
  } = useWidgetPlotContext();

  const { timeRangeTo, timeRangeFrom } = useStatsHouseShallow(selectorStore);

  const metricMeta = useMetricName(true);
  const divOut = useRef<HTMLDivElement>(null);
  const divInner = useRef<HTMLDivElement>(null);

  const [visibleRef, setVisibleRef] = useState<HTMLElement | null>(null);
  const visible = useIntersectionObserver(visibleRef, 0, undefined, 0);
  const visibleBool = visible > 0;

  const meta = useMetricMeta(metricMeta, !isDashboard && visibleBool);

  const iconVisible = usePlotVisibilityStore(useCallback(({ plotPreviewList }) => !!plotPreviewList[id], [id]));

  const [
    {
      error403,
      topInfo,
      dataView,
      series,
      seriesShow,
      metricUnit: plotDataMetricUnit,
      whats: plotDataWhat,
      legendNameWidth,
      legendValueWidth,
      legendMaxHostWidth,
      legendMaxDotSpaceWidth,
    },
    setData,
  ] = useMetricData(visibleBool || iconVisible, visibleBool ? (isDashboard ? 2 : 1) : 3);

  const metricUnitData = plotDataMetricUnit ?? meta?.metric_type;

  const themeDark = useThemeStore((s) => s.dark);
  const compact = isDashboard || isEmbed;
  const yLockRef = useStateToRef(yLock ?? yLockDefault);
  const getAxisStroke = useCallback(() => (themeDark ? grey : black), [themeDark]);

  const [cursorLock, setCursorLock] = useState(false);

  const uPlotRef = useRef<uPlot>(undefined);
  const [legend, setLegend] = useState<LegendItem<PlotValues>[]>([]);

  const [pluginEventOverlay, pluginEventOverlayHooks] = useUPlotPluginHooks();

  const topPad = compact ? 8 : 16;
  const xAxisSize = compact ? 32 : 48;

  const resetZoomRef = useStateToRef(resetZoom);

  const onSetSelect = useCallback(
    (u: uPlot) => {
      if (u.status === 1) {
        const xMin = u.posToVal(u.select.left, 'x');
        const xMax = u.posToVal(u.select.left + u.select.width, 'x');
        const yMin = u.posToVal(u.select.top + u.select.height, 'y');
        const yMax = u.posToVal(u.select.top, 'y');
        const xOnly = u.select.top === 0;
        if (!xOnly) {
          setPlotYLock(id, true, { min: yMin, max: yMax });
        } else {
          setLiveMode(false);
          setTimeRange({ from: Math.floor(xMin), to: Math.ceil(xMax) });
        }
      }
    },
    [id]
  );

  const metricType = useMemo(() => {
    if (metricUnit != null) {
      return metricUnit;
    }

    return getMetricType(plotDataWhat?.length ? plotDataWhat : plotWhat, metricUnitData);
  }, [metricUnit, metricUnitData, plotDataWhat, plotWhat]);

  const opts = useMemo<UPlotWrapperPropsOpts>(() => {
    const grid: uPlot.Axis.Grid = {
      stroke: themeDark ? greyDark : grey,
      width: 1 / devicePixelRatio,
    };

    return {
      pxAlign: false, // avoid shimmer in live mode
      padding: [topPad, rightPad, 0, 0],
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
        sync: isDashboard
          ? {
              key: syncGroup,
              filters: {
                sub(event) {
                  return event !== 'mouseup' && event !== 'mousedown';
                },
                pub(event) {
                  return event !== 'mouseup' && event !== 'mousedown';
                },
              },
            }
          : undefined,
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
          values: (_, splits) => splits.map(formatByMetricType(metricType)),
          filter: logScale ? log2Filter : undefined,
          size: getYAxisSize(yAxisSize),
          font: font,
          stroke: getAxisStroke,
          splits: !logScale
            ? undefined
            : (
                self: uPlot,
                axisIdx: number,
                scaleMin: number,
                scaleMax: number,
                foundIncr: number,
                foundSpace: number
              ) => log2Splits(self, axisIdx, scaleMin, scaleMax, foundIncr, foundSpace),
          incrs: metricTypeIncrs[metricType],
        },
      ],
      scales: {
        x: { auto: false, range: xRangeStatic },
        y: {
          distr: logScale ? 100 : 1,
          bwd,
          fwd,
          auto: () => !yLockRef.current || (yLockRef.current.min === 0 && yLockRef.current.max === 0),

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
          value: dateRangeFormat,
        },
      ],
      legend: {
        show: false,
        live: true, //!compact,
        markers: {
          width: devicePixelRatio > 1 ? 1.5 : 1,
        },
      },
      plugins: [pluginEventOverlay],
    };
  }, [
    compact,
    getAxisStroke,
    isDashboard,
    metricType,
    pluginEventOverlay,
    themeDark,
    topPad,
    xAxisSize,
    logScale,
    yLockRef,
  ]);

  const onReady = useCallback(
    (u: uPlot) => {
      if (uPlotRef.current !== u) {
        uPlotRef.current = u;
      }
      // setUPlotWidth(indexPlot, u.bbox.width);
      u.over.onclick = () => {
        setCursorLock((u.cursor as { _lock: boolean })._lock);
      };
      u.over.ondblclick = () => {
        resetZoomRef.current(id);
      };
      u.setCursor({ top: -10, left: -10 }, false);
    },
    [id, resetZoomRef]
  );

  const onUpdatePreview = useCallback(
    (u: uPlot) => {
      if (isDashboard && !isEmbed) {
        createPlotPreview(id, u);
      }
    },
    [isDashboard, isEmbed, id]
  );

  const scales = useMemo<UPlotWrapperPropsScales>(() => {
    const res: UPlotWrapperPropsScales = {};
    res.x = { min: timeRangeFrom + timeRangeTo, max: timeRangeTo };
    if (yLock && (yLock.min !== 0 || yLock.max !== 0)) {
      res.y = { ...yLock };
    }
    return res;
  }, [timeRangeFrom, timeRangeTo, yLock]);

  const [fixHeight, setFixHeight] = useState<number>(0);
  const [fixInnerWidth, setFixInnerWidth] = useState<number>(0);
  const [fixInnerHeight, setFixInnerHeight] = useState<number>(0);

  const onMouseOver = useCallback(() => {
    if (divOut.current && divInner.current && !isEmbed) {
      setFixHeight(divOut.current.getBoundingClientRect().height);
      setFixInnerWidth(divInner.current.getBoundingClientRect().width);
      setFixInnerHeight(divInner.current.getBoundingClientRect().height);
    }
  }, [isEmbed, setFixHeight]);
  const onMouseOut = useCallback(() => {
    setFixHeight(0);
    setFixInnerWidth(0);
    setFixInnerHeight(0);
  }, [setFixHeight]);

  const onLegendFocus = useCallback((index: number, focus: boolean) => {
    if ((uPlotRef.current?.cursor as { _lock: boolean })._lock) {
      return;
    }
    uPlotRef.current?.setSeries(index, { focus }, true);
  }, []);

  const onLegendShow = useCallback(
    (index: number, show: boolean, single: boolean) => {
      const idx = index - 1;
      setData(
        produce<PlotData>((d) => {
          if (single) {
            const otherShow = d.seriesShow.some((_show, indexSeries) => (indexSeries === idx ? false : _show));
            d.seriesShow = d.seriesShow.map((_, indexSeries) => (indexSeries === idx ? true : !otherShow));
          } else {
            d.seriesShow[idx] = show ?? !d.seriesShow[idx];
          }
        })
      );
    },
    [setData]
  );

  useEffect(() => {
    seriesShow?.forEach((show, idx) => {
      if (uPlotRef.current?.series[idx + 1] && uPlotRef.current?.series[idx + 1].show !== show) {
        uPlotRef.current?.setSeries(idx + 1, { show }, true);
      }
    });
  }, [seriesShow]);
  useEffect(() => {
    setPlotVisibility(id, visibleBool);
  }, [id, visibleBool]);

  const onUpdateLegend = useCallback<React.Dispatch<React.SetStateAction<LegendItem<PlotValues>[]>>>(
    (legend) => {
      setLegend((prevLegend) => {
        const nextLegend = typeof legend === 'function' ? legend(prevLegend) : legend;
        if (fixHeight > 0 || !isDashboard || nextLegend.some((v, index) => v.label !== prevLegend[index]?.label)) {
          return nextLegend;
        }
        return prevLegend;
      });
    },
    [fixHeight, isDashboard]
  );

  return (
    <div
      className={cn(
        'plot-view ',
        compact ? 'plot-compact' : 'plot-full',
        isDashboard && 'plot-dash',
        fixHeight > 0 && isDashboard && 'plot-hover',
        className
      )}
      ref={divOut}
      style={
        {
          '--legend-name-width': `${legendNameWidth}px`,
          '--legend-value-width': `${legendValueWidth}px`,
          '--legend-max-host-width': `${legendMaxHostWidth}px`,
          '--legend-dot-space-width': `${legendMaxDotSpaceWidth}px`,
          height: fixHeight > 0 && isDashboard ? `${fixHeight}px` : undefined,
        } as React.CSSProperties
      }
      onMouseOver={onMouseOver}
      onMouseOut={onMouseOut}
    >
      <div data-plot-key={id} ref={setVisibleRef} className={cn('plot-view-inner', !fixRatio && 'd-flex flex-column')}>
        <div
          ref={divInner}
          className={cn('d-flex flex-column', !fixRatio && 'flex-grow-1 h-0')}
          style={{
            height: fixInnerHeight > 0 && isDashboard ? `${fixInnerHeight}px` : undefined,
            width: fixInnerWidth > 0 && isDashboard ? `${fixInnerWidth}px` : undefined,
          }}
        >
          <div
            className="d-flex align-items-center position-relative"
            style={{
              marginRight: `${rightPad}px`,
            }}
          >
            {/*loader*/}
            <div
              style={{ width: `${yAxisSize}px` }}
              className="flex-shrink-0 d-flex justify-content-end align-items-center pe-3"
            >
              <PlotHealsStatus />
            </div>
            {/*header*/}
            <div className="d-flex flex-column flex-grow-1 w-0 overflow-force-wrap">
              <PlotHeader isDashboard={isDashboard} isEmbed={isEmbed} />
              {!compact && <PlotSubMenu />}
            </div>
          </div>
          <PlotBox className="z-1" fixRatio={fixRatio}>
            {error403 ? (
              <div className="text-bg-light w-100 h-100 position-absolute top-0 start-0 d-flex align-items-center justify-content-center">
                Access denied
              </div>
            ) : (
              <UPlotWrapper
                opts={opts}
                data={dataView}
                series={series}
                scales={scales}
                onReady={onReady}
                onSetSelect={onSetSelect}
                onUpdatePreview={onUpdatePreview}
                className={cn('w-100 h-100 position-absolute top-0 start-0', cursorLock && css.cursorLock)}
                onUpdateLegend={onUpdateLegend}
              >
                {events.length > 0 && (
                  <UPlotPluginPortal hooks={pluginEventOverlayHooks} zone="over">
                    <PlotEventOverlay
                      hooks={pluginEventOverlayHooks}
                      flagHeight={Math.min(topPad, 10)}
                      compact={compact}
                    />
                  </UPlotPluginPortal>
                )}
              </UPlotWrapper>
            )}
          </PlotBox>
        </div>
        {!error403 && (
          <div className="plot-legend">
            <PlotLegend
              plotKey={id}
              legend={legend}
              onLegendShow={onLegendShow}
              onLegendFocus={onLegendFocus}
              compact={compact && !(fixHeight > 0 && isDashboard)}
              unit={metricType}
            />
            {topInfo && (!compact || (fixHeight > 0 && isDashboard)) && (
              <div className="pb-3">
                Showing {numSeries > 0 ? 'top' : 'bottom'} {topInfo.top} out of {topInfo.total} total series
                {topInfo.info}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
