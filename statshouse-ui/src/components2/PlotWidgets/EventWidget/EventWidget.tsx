// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { PlotWidgetRouterProps } from '../PlotWidgetRouter';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useIntersectionObserver, useStateToRef, useUPlotPluginHooks } from '@/hooks';
import { useThemeStore } from '@/store2/themeStore';
import { black, grey, greyDark } from '@/view/palette';
import uPlot from 'uplot';
import { setLiveMode } from '@/store2/liveModeStore';
import { formatByMetricType, getMetricType } from '@/common/formatByMetricType';
import {
  UPlotPluginPortal,
  UPlotWrapper,
  UPlotWrapperPropsOpts,
  UPlotWrapperPropsScales,
} from '@/components/UPlotWrapper';
import { dataIdxNearest } from '@/common/dataIdxNearest';
import { font, getYAxisSize, xAxisValues, xAxisValuesCompact } from '@/common/axisValues';
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
import { PlotEvents } from '@/components2/Plot/PlotView/PlotEvents';
import { useMetricName } from '@/hooks/useMetricName';
import { useMetricMeta } from '@/hooks/useMetricMeta';
import { useMetricData } from '@/hooks/useMetricData';
import { resetZoom, setPlotYLock, setTimeRange } from '@/store2/methods';
import { type StatsHouseStore, useStatsHouseShallow } from '@/store2';
import { PlotBox } from '@/components2/Plot/PlotView/PlotBox';

const rightPad = 16;

const unFocusAlfa = 1;
const yLockDefault = { min: 0, max: 0 };
const syncGroup = '1';

const selectorStore = ({
  params: {
    timeRange: { to, from },
  },
}: StatsHouseStore) => ({ timeRangeTo: to, timeRangeFrom: from });

export function EventWidget({ className, isDashboard, isEmbed, fixRatio }: PlotWidgetRouterProps) {
  const {
    plot: { id, what: plotWhat, yLock, metricUnit },
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
      dataView,
      series,
      seriesShow,
      metricUnit: plotDataMetricUnit,
      whats: plotDataWhat,
      legendNameWidth,
      legendValueWidth,
      legendMaxHostWidth,
      legendMaxDotSpaceWidth,
      bands,
    },
  ] = useMetricData(visibleBool || iconVisible, visibleBool ? (isDashboard ? 2 : 1) : 3);

  const metricUnitData = plotDataMetricUnit ?? meta?.metric_type;

  const themeDark = useThemeStore((s) => s.dark);
  const [plotEventsDataRange, setPlotEventsDataRange] = useState<{ from: number; to: number } | undefined>(undefined);

  const compact = isDashboard || isEmbed;
  const yLockRef = useStateToRef(yLock ?? yLockDefault);
  const getAxisStroke = useCallback(() => (themeDark ? grey : black), [themeDark]);

  const [cursorLock, setCursorLock] = useState(false);

  const uPlotRef = useRef<uPlot>(undefined);

  const [pluginTimeWindow, pluginTimeWindowHooks] = useUPlotPluginHooks();

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
        sync: {
          key: syncGroup,
          filters: {
            sub(event) {
              return event !== 'mouseup' && event !== 'mousedown';
            },
            pub(event) {
              return event !== 'mouseup' && event !== 'mousedown';
            },
          },
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
          values: (_, splits) => splits.map(formatByMetricType(metricType)),
          size: getYAxisSize(yAxisSize),
          font: font,
          stroke: getAxisStroke,
          incrs: metricTypeIncrs[metricType],
        },
      ],
      scales: {
        x: { auto: false, range: xRangeStatic },
        y: {
          auto: (_) => !yLockRef.current || (yLockRef.current.min === 0 && yLockRef.current.max === 0),
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
      plugins: [pluginTimeWindow],
    };
  }, [compact, getAxisStroke, metricType, pluginTimeWindow, themeDark, topPad, xAxisSize, yLockRef]);

  const onReady = useCallback(
    (u: uPlot) => {
      if (uPlotRef.current !== u) {
        uPlotRef.current = u;
      }
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
    [id, isDashboard, isEmbed]
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
      setFixInnerHeight(divInner.current.getBoundingClientRect().height);
      setFixInnerWidth(divInner.current.getBoundingClientRect().width);
    }
  }, [isEmbed]);
  const onMouseOut = useCallback(() => {
    setFixHeight(0);
    setFixInnerHeight(0);
    setFixInnerWidth(0);
  }, []);

  const timeWindow = useMemo(() => {
    let leftWidth = 0;
    const rightWidth = 0;
    if (uPlotRef.current && plotEventsDataRange) {
      leftWidth =
        (Math.max(
          0,
          Math.round(uPlotRef.current.valToPos(Math.min(plotEventsDataRange.from, plotEventsDataRange.to), 'x'))
        ) /
          uPlotRef.current.over.clientWidth) *
        100;
    }
    return {
      leftWidth: `${leftWidth}%`,
      rightWidth: `${rightWidth}%`,
    };
  }, [plotEventsDataRange]);

  const [cursorTime, setCursorTime] = useState<number>();

  const onSetCursor = useCallback((u: uPlot) => {
    if (u.cursor.idx !== null && u.cursor.idx !== undefined) {
      setCursorTime(u.data[0][u.legend.idxs?.[0] ?? -1]);
    } else {
      setCursorTime(undefined);
    }
  }, []);

  const onCursor = useCallback((time: number) => {
    if (uPlotRef.current) {
      uPlotRef.current.setCursor({ top: -1, left: uPlotRef.current?.valToPos(time, 'x') }, false);
    }
  }, []);

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

  return (
    <div
      className={cn(
        'plot-view',
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
          className={cn('d-flex flex-column', !fixRatio && 'flex-grow-1')}
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
                bands={bands}
                series={series}
                scales={scales}
                onReady={onReady}
                onSetSelect={onSetSelect}
                onUpdatePreview={onUpdatePreview}
                className={cn('w-100 h-100 position-absolute top-0 start-0', cursorLock && css.cursorLock)}
                onSetCursor={onSetCursor}
              >
                {!compact && (
                  <UPlotPluginPortal zone="over" hooks={pluginTimeWindowHooks}>
                    <>
                      {timeWindow.leftWidth !== '0%' && (
                        <div className="u-time-window u-time-window-left" style={{ width: timeWindow.leftWidth }}></div>
                      )}
                      {timeWindow.rightWidth !== '0%' && (
                        <div
                          className="u-time-window u-time-window-right"
                          style={{ width: timeWindow.rightWidth }}
                        ></div>
                      )}
                    </>
                  </UPlotPluginPortal>
                )}
              </UPlotWrapper>
            )}
          </PlotBox>
        </div>
        {!error403 && !compact && (
          <PlotEvents
            key={id}
            className="plot-legend flex-grow-1"
            onCursor={onCursor}
            cursor={cursorTime}
            setTimeRange={setPlotEventsDataRange}
          />
        )}
      </div>
    </div>
  );
}
