// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { type PlotViewProps } from './PlotView';
import { useStatsHouse, useStatsHouseShallow } from 'store2';
import { useThemeStore } from 'store2/themeStore';
import { useIntersectionObserver, useStateToRef, useUPlotPluginHooks } from 'hooks';
import { black, grey, greyDark } from 'view/palette';
import {
  UPlotPluginPortal,
  UPlotWrapper,
  UPlotWrapperPropsOpts,
  UPlotWrapperPropsScales,
} from 'components/UPlotWrapper';
import { formatByMetricType, getMetricType, splitByMetricType } from 'common/formatByMetricType';
import { dataIdxNearest } from 'common/dataIdxNearest';
import { font, getYAxisSize, xAxisValues, xAxisValuesCompact } from 'common/axisValues';
import { yAxisSize } from 'common/settings';
import { METRIC_TYPE } from 'api/enum';
import { xRangeStatic } from './xRangeStatic';
import { calcYRange } from 'common/calcYRange';
import { dateRangeFormat } from './dateRangeFormat';
import cn from 'classnames';
import { PlotHealsStatus } from './PlotHealsStatus';
import { PlotHeader } from './PlotHeader';
import { PlotSubMenu } from './PlotSubMenu';
import css from './style.module.css';
import { incrs } from './constants';
import { PlotEvents } from './PlotEvents';
import uPlot from 'uplot';
import { setLiveMode } from 'store2/liveModeStore';
import { setPlotVisibility } from 'store2/plotVisibilityStore';
import { createPlotPreview } from 'store2/plotPreviewStore';

const rightPad = 16;

// const themeDark = false;
// const xAxisSize = 16;
const unFocusAlfa = 1;
const yLockDefault = { min: 0, max: 0 };
const syncGroup = '1';

const { setPlotYLock, setTimeRange, resetZoom } = useStatsHouse.getState();

export function PlotViewEvent({ plotKey, className, isDashboard }: PlotViewProps) {
  const {
    yLock,

    error403,
    isEmbed,
    metricUnit,
    metricUnitData,

    dataView,
    bands,
    series,
    // scales,
    timeRangeTo,
    timeRangeFrom,
    seriesShow,
    plotWhat,
    plotDataWhat,
    legendNameWidth,
    legendValueWidth,
    legendMaxHostWidth,
    legendMaxDotSpaceWidth,
    plotEventsDataRange,
    isActive,
    // setPlotVisibility,
    // setPlotYLock,
    // setTimeRange,
    // createPlotPreview,
    // resetZoom,
  } = useStatsHouseShallow(
    ({
      plotsData,
      params: { tabNum, plots, timeRange },
      plotsEventsData,
      metricMeta,
      isEmbed,
      baseRange,
      // setPlotVisibility,
      // setPlotYLock,
      // setTimeRange,
      // createPlotPreview,
      // resetZoom,
    }) => {
      const plot = plots[plotKey];
      const plotData = plotsData[plotKey];
      return {
        plotWhat: plot?.what,
        plotDataWhat: plotData?.whats,
        yLock: plot?.yLock,
        timeRangeTo: timeRange.to,
        timeRangeFrom: timeRange.from,
        error403: plotData?.error403 ?? '',
        metricUnit: plot?.metricUnit,
        metricUnitData: plotData?.metricUnit ?? metricMeta[plot?.metricName ?? '']?.metric_type,
        dataView: plotData?.dataView,
        bands: plotData?.bands,
        series: plotData?.series,
        // scales: plotData?.scales,
        seriesShow: plotData?.seriesShow,
        legendNameWidth: plotData?.legendNameWidth,
        legendValueWidth: plotData?.legendValueWidth,
        legendMaxHostWidth: plotData?.legendMaxHostWidth,
        legendMaxDotSpaceWidth: plotData?.legendMaxDotSpaceWidth,
        plotEventsDataRange: plotsEventsData[plotKey]?.range,
        isEmbed,
        baseRange,
        isActive: tabNum === plotKey,
        // setPlotVisibility,
        // setPlotYLock,
        // setTimeRange,
        // createPlotPreview,
        // resetZoom,
      };
    }
  );
  const divOut = useRef<HTMLDivElement>(null);
  const [visibleRef, setVisibleRef] = useState<HTMLElement | null>(null);
  const visible = useIntersectionObserver(visibleRef, 0, undefined, 0);
  const visibleBool = visible > 0;
  const themeDark = useThemeStore((s) => s.dark);
  const compact = isDashboard || isEmbed;
  const yLockRef = useStateToRef(yLock ?? yLockDefault);
  const getAxisStroke = useCallback(() => (themeDark ? grey : black), [themeDark]);

  const [cursorLock, setCursorLock] = useState(false);

  const uPlotRef = useRef<uPlot>();

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
          setPlotYLock(plotKey, true, { min: yMin, max: yMax });
        } else {
          setLiveMode(false);
          setTimeRange({ from: Math.floor(xMin), to: Math.ceil(xMax) });
        }
      }
    },
    [plotKey]
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
          splits: metricType === METRIC_TYPE.none ? undefined : splitByMetricType(metricType),
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
      plugins: [pluginTimeWindow],
    };
  }, [compact, getAxisStroke, metricType, pluginTimeWindow, themeDark, topPad, xAxisSize, yLockRef]);

  const onReady = useCallback(
    (u: uPlot) => {
      if (uPlotRef.current !== u) {
        uPlotRef.current = u;
      }
      // setUPlotWidth(indexPlot, u.bbox.width);
      u.over.onclick = () => {
        // @ts-ignore
        setCursorLock(u.cursor._lock);
      };
      u.over.ondblclick = () => {
        resetZoomRef.current(plotKey);
      };
      u.setCursor({ top: -10, left: -10 }, false);
    },
    [plotKey, resetZoomRef]
  );

  const onUpdatePreview = useCallback(
    (u: uPlot) => {
      if (isDashboard && !isEmbed) {
        createPlotPreview(plotKey, u);
      }
    },
    [isDashboard, isEmbed, plotKey]
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
  const onMouseOver = useCallback(() => {
    if (divOut.current && !isEmbed) {
      setFixHeight(divOut.current.getBoundingClientRect().height);
    }
  }, [isEmbed]);
  const onMouseOut = useCallback(() => {
    setFixHeight(0);
  }, []);

  const timeWindow = useMemo(() => {
    let leftWidth = 0;
    let rightWidth = 0;
    if (uPlotRef.current && plotEventsDataRange) {
      leftWidth =
        (Math.max(
          0,
          Math.round(uPlotRef.current.valToPos(Math.min(plotEventsDataRange.from, plotEventsDataRange.to), 'x'))
        ) /
          uPlotRef.current.over.clientWidth) *
        100;
      rightWidth =
        100 -
        (Math.max(
          0,
          Math.round(uPlotRef.current.valToPos(Math.max(plotEventsDataRange.from, plotEventsDataRange.to), 'x'))
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
    if (isDashboard || isActive) {
      setPlotVisibility(plotKey, visibleBool);
    }
  }, [isActive, isDashboard, plotKey, visibleBool]);

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
      <div data-plot-key={plotKey} ref={setVisibleRef} className="plot-view-inner">
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
            <PlotHealsStatus plotKey={plotKey} />
          </div>
          {/*header*/}
          <div className="d-flex flex-column flex-grow-1 overflow-force-wrap">
            <PlotHeader plotKey={plotKey} isDashboard={isDashboard} />
            {!compact && <PlotSubMenu plotKey={plotKey} />}
          </div>
        </div>
        <div
          className="position-relative w-100 z-1"
          style={
            {
              // paddingTop: '15%',
              paddingTop: '55%',
              '--plot-padding-top': `${topPad}px`,
            } as React.CSSProperties
          }
        >
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
                      <div className="u-time-window u-time-window-right" style={{ width: timeWindow.rightWidth }}></div>
                    )}
                  </>
                </UPlotPluginPortal>
              )}
            </UPlotWrapper>
          )}
        </div>
        {!error403 && !compact && (
          <PlotEvents className="plot-legend flex-grow-1" plotKey={plotKey} onCursor={onCursor} cursor={cursorTime} />
        )}
      </div>
    </div>
  );
}
