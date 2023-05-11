// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import uPlot from 'uplot';
import { calcYRange } from '../../common/calcYRange';
import { PlotSubMenu } from './PlotSubMenu';
import { PlotHeader } from './PlotHeader';
import { UPlotWrapper, UPlotWrapperPropsOpts } from '../index';
import { formatSI, now, timeRangeAbbrevExpand } from '../../view/utils';
import { queryURLCSV } from '../../view/api';
import { black, grey, greyDark } from '../../view/palette';
import produce from 'immer';
import {
  selectorBaseRange,
  selectorEventsByIndex,
  selectorLiveMode,
  selectorLoadEvents,
  selectorLoadMetricsMeta,
  selectorMetricsMetaByName,
  selectorNumQueriesPlotByIndex,
  selectorParamsPlotsByIndex,
  selectorParamsTimeShifts,
  selectorPlotLastError,
  selectorPlotsDataByIndex,
  selectorSetLiveMode,
  selectorSetParamsPlots,
  selectorSetPreviews,
  selectorSetTimeRange,
  selectorSetUPlotWidth,
  selectorSetYLockChange,
  selectorThemeDark,
  selectorTimeRange,
  selectorUPlotsWidthByIndex,
  useStore,
} from '../../store';
import { xAxisValues, xAxisValuesCompact } from '../../common/axisValues';
import cn from 'classnames';
import { PlotEvents } from './PlotEvents';
import { useUPlotPluginHooks } from '../../hooks';
import { UPlotPluginPortal } from '../UPlotWrapper';

const unFocusAlfa = 1;
const rightPad = 16;
const font =
  '12px system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, "Noto Sans", "Liberation Sans", sans-serif'; // keep in sync with $font-family-sans-serif

function xRangeStatic(u: uPlot, dataMin: number | null, dataMax: number | null): [number, number] {
  if (dataMin === null || dataMax === null) {
    const t = now();
    return [t - 3600, t];
  }
  return [dataMin, dataMax];
}

export function PlotViewEvent(props: {
  indexPlot: number;
  className?: string;
  dashboard?: boolean;
  compact: boolean;
  yAxisSize: number;
  group?: string;
}) {
  const { indexPlot, compact, yAxisSize, dashboard, className, group } = props;

  const selectorParamsPlot = useMemo(() => selectorParamsPlotsByIndex.bind(undefined, indexPlot), [indexPlot]);
  const sel = useStore(selectorParamsPlot);
  const setParamsPlots = useStore(selectorSetParamsPlots);
  const setSel = useMemo(() => setParamsPlots.bind(undefined, indexPlot), [indexPlot, setParamsPlots]);
  const selectorEvent = useMemo(() => selectorEventsByIndex.bind(undefined, indexPlot), [indexPlot]);
  const plotEvent = useStore(selectorEvent);
  const [pluginTimeWindow, pluginTimeWindowHooks] = useUPlotPluginHooks();

  const timeShifts = useStore(selectorParamsTimeShifts);

  const timeRange = useStore(selectorTimeRange);
  const setTimeRange = useStore(selectorSetTimeRange);

  const baseRange = useStore(selectorBaseRange);

  const setPreviewImage = useStore(selectorSetPreviews);

  const live = useStore(selectorLiveMode);
  const setLive = useStore(selectorSetLiveMode);

  const selectorPlotsData = useMemo(() => selectorPlotsDataByIndex.bind(undefined, indexPlot), [indexPlot]);
  const {
    scales,
    series,
    seriesShow,
    data,
    legendNameWidth,
    legendValueWidth,
    legendMaxDotSpaceWidth,
    legendMaxHostWidth,
    mappingFloodEvents,
    samplingFactorSrc,
    samplingFactorAgg,
    receiveErrors,
    error: lastError,
    error403,
  } = useStore(selectorPlotsData);

  const setYLockChange = useStore(selectorSetYLockChange);
  const onYLockChange = useMemo(() => setYLockChange?.bind(undefined, indexPlot), [indexPlot, setYLockChange]);

  const setLastError = useStore(selectorPlotLastError);
  const selectorNumQueries = useMemo(() => selectorNumQueriesPlotByIndex.bind(undefined, indexPlot), [indexPlot]);
  const numQueries = useStore(selectorNumQueries);

  const selectorUPlotWidth = useMemo(() => selectorUPlotsWidthByIndex.bind(undefined, indexPlot), [indexPlot]);
  const width = useStore(selectorUPlotWidth);
  const setUPlotWeight = useStore(selectorSetUPlotWidth);

  const selectorPlotMetricsMeta = useMemo(
    () => selectorMetricsMetaByName.bind(undefined, sel.metricName ?? ''),
    [sel.metricName]
  );
  const meta = useStore(selectorPlotMetricsMeta);
  const loadMetricsMeta = useStore(selectorLoadMetricsMeta);

  const loadEvent = useStore(selectorLoadEvents);

  const themeDark = useStore(selectorThemeDark);

  const uPlotRef = useRef<uPlot>();

  useEffect(() => {
    if (sel.metricName) {
      loadMetricsMeta(sel.metricName);
    }
  }, [sel.metricName, loadMetricsMeta]);

  const clearLastError = useCallback(() => {
    setLastError(indexPlot, '');
  }, [indexPlot, setLastError]);

  const resetZoom = useCallback(() => {
    setSel(
      produce((s) => {
        s.yLock = { min: 0, max: 0 };
      })
    );
    setTimeRange(timeRangeAbbrevExpand(baseRange, now()));
  }, [setSel, setTimeRange, baseRange]);

  const topPad = compact ? 8 : 16;
  const xAxisSize = compact ? 32 : 48;

  // make sure "create plot" does not depend on lock range
  const yLockRef = useRef(sel.yLock);

  useEffect(() => {
    yLockRef.current = sel.yLock;
  }, [sel.yLock]);

  // all of this so that our "create plot" layout effect does not depend on the time range
  const resetZoomRef = useRef(resetZoom);
  useEffect(() => {
    resetZoomRef.current = resetZoom;
  }, [resetZoom]);

  const onSetSelect = useCallback(
    (u: uPlot) => {
      if (u.status === 1) {
        const xMin = u.posToVal(u.select.left, 'x');
        const xMax = u.posToVal(u.select.left + u.select.width, 'x');
        const yMin = u.posToVal(u.select.top + u.select.height, 'y');
        const yMax = u.posToVal(u.select.top, 'y');
        const xOnly = u.select.top === 0;
        if (!xOnly) {
          setSel(
            produce((s) => {
              s.yLock = { min: yMin, max: yMax }; // unfortunately will cause a re-scale from the effect
            })
          );
        } else {
          setLive(false);
          setTimeRange({ from: Math.floor(xMin), to: Math.ceil(xMax) });
        }
      }
    },
    [setLive, setSel, setTimeRange]
  );

  const getAxisStroke = useCallback(() => (themeDark ? grey : black), [themeDark]);

  const opts = useMemo<UPlotWrapperPropsOpts>(() => {
    const grid: uPlot.Axis.Grid = {
      stroke: themeDark ? greyDark : grey,
      width: 1 / devicePixelRatio,
    };
    const sync: uPlot.Cursor.Sync | undefined = group
      ? {
          key: group,
        }
      : undefined;
    return {
      pxAlign: false, // avoid shimmer in live mode
      padding: [topPad, rightPad, 0, 0],
      cursor: {
        lock: false,
        drag: {
          dist: 5, // try to prevent double-click-selections a bit
          x: true,
          y: true,
          uni: Infinity,
        },
        // focus: {
        //   prox: Infinity, // always have one series focused
        // },
        sync,
        // dataIdx: dataIdxNearest,
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
          values: (_, splits) => splits.map(formatSI),
          size: yAxisSize,
          font: font,
          stroke: getAxisStroke,
        },
      ],
      scales: {
        x: { auto: false, range: xRangeStatic },
        y: {
          auto: false,
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
          value: '{DD}/{MM}/{YY} {H}:{mm}:{ss}',
        },
      ],
      legend: {
        show: false,
        live: false, //!compact,
        markers: {
          width: devicePixelRatio > 1 ? 1.5 : 1,
        },
      },
      plugins: [pluginTimeWindow],
    };
  }, [compact, getAxisStroke, group, pluginTimeWindow, themeDark, topPad, xAxisSize, yAxisSize]);

  const timeWindow = useMemo(() => {
    let leftWidth = 0;
    let rightWidth = 0;
    if (uPlotRef.current) {
      leftWidth =
        (Math.max(0, Math.round(uPlotRef.current.valToPos(Math.min(plotEvent.range.from, plotEvent.range.to), 'x'))) /
          uPlotRef.current.over.clientWidth) *
        100;
      rightWidth =
        100 -
        (Math.max(0, Math.round(uPlotRef.current.valToPos(Math.max(plotEvent.range.from, plotEvent.range.to), 'x'))) /
          uPlotRef.current.over.clientWidth) *
          100;
    }
    return {
      leftWidth: `${leftWidth}%`,
      rightWidth: `${rightWidth}%`,
    };
  }, [plotEvent.range.from, plotEvent.range.to]);

  const linkCSV = useMemo(() => {
    const agg =
      sel.customAgg === -1
        ? `${Math.floor(width / 2)}`
        : sel.customAgg === 0
        ? `${Math.floor(width * devicePixelRatio)}`
        : `${sel.customAgg}s`;
    return queryURLCSV(sel, timeRange, timeShifts, agg);
  }, [sel, timeRange, timeShifts, width]);

  const onReady = useCallback(
    (u: uPlot) => {
      if (uPlotRef.current !== u) {
        uPlotRef.current = u;
      }
      setUPlotWeight(indexPlot, u.bbox.width);
      u.over.ondblclick = () => {
        resetZoomRef.current();
      };
      u.over.onclick = () => {
        loadEvent(indexPlot, undefined, false, u.data[0]?.[u.cursor.idx ?? 0]).catch(() => undefined);
      };
      u.setCursor({ top: -10, left: -10 }, false);
    },
    [indexPlot, loadEvent, setUPlotWeight]
  );

  const onUpdatePreview = useMemo(() => setPreviewImage?.bind(undefined, indexPlot), [indexPlot, setPreviewImage]);

  const [fixHeight, setFixHeight] = useState<number>(0);
  const divOut = useRef<HTMLDivElement>(null);
  const onMouseOver = useCallback(() => {
    if (divOut.current) {
      setFixHeight(divOut.current.getBoundingClientRect().height);
    }
  }, []);
  const onMouseOut = useCallback(() => {
    setFixHeight(0);
  }, []);

  const [cursorTime, setCursorTime] = useState<number>();

  const onSetCursor = useCallback((u: uPlot) => {
    setCursorTime(u.data[0][u.cursor.idx ?? -1]);
  }, []);

  const onCursor = useCallback((time: number) => {
    if (uPlotRef.current) {
      uPlotRef.current.setCursor({ top: -1, left: uPlotRef.current?.valToPos(time, 'x') }, false);
    }
  }, []);

  useEffect(() => {
    seriesShow.forEach((show, idx) => {
      if (uPlotRef.current?.series[idx + 1].show !== show) {
        uPlotRef.current?.setSeries(idx + 1, { show }, true);
      }
    });
  }, [seriesShow]);

  return (
    <div
      className={cn(
        'plot-view',
        compact ? 'plot-compact' : 'plot-full',
        dashboard && 'plot-dash',
        fixHeight > 0 && dashboard && 'plot-hover',
        className
      )}
      ref={divOut}
      style={
        {
          '--legend-name-width': `${legendNameWidth}px`,
          '--legend-value-width': `${legendValueWidth}px`,
          '--legend-max-host-width': `${legendMaxHostWidth}px`,
          '--legend-dot-space-width': `${legendMaxDotSpaceWidth}px`,
          height: fixHeight > 0 && dashboard ? `${fixHeight}px` : undefined,
        } as React.CSSProperties
      }
      onMouseOver={onMouseOver}
      onMouseOut={onMouseOut}
    >
      <div className="plot-view-inner">
        <div className="d-flex align-items-center position-relative z-2" style={{ marginRight: `${rightPad}px` }}>
          {/*loader*/}
          <div
            style={{ width: `${yAxisSize}px` }}
            className="flex-shrink-0 d-flex justify-content-end align-items-center pe-3"
          >
            {numQueries > 0 && compact && (
              <div className="text-info spinner-border spinner-border-sm" role="status" aria-hidden="true" />
            )}
          </div>
          {/*header*/}
          <div className="d-flex flex-column flex-grow-1 overflow-force-wrap">
            {/*last error*/}
            {!!lastError && (
              <div className="alert alert-danger d-flex align-items-center justify-content-between" role="alert">
                <small className="overflow-force-wrap font-monospace">{lastError}</small>
                <button type="button" className="btn-close" aria-label="Close" onClick={clearLastError} />
              </div>
            )}
            <PlotHeader
              indexPlot={indexPlot}
              sel={sel}
              setParams={setSel}
              setLive={setLive}
              meta={meta}
              live={live}
              yLock={sel.yLock}
              setTimeRange={setTimeRange}
              onResetZoom={resetZoom}
              onYLockChange={onYLockChange}
              dashboard={dashboard}
              compact={compact}
            />
            {!compact && (
              /*meta*/
              <PlotSubMenu
                linkCSV={linkCSV}
                mappingFloodEvents={mappingFloodEvents}
                timeRange={timeRange}
                sel={sel}
                receiveErrors={receiveErrors}
                samplingFactorAgg={samplingFactorAgg}
                samplingFactorSrc={samplingFactorSrc}
              />
            )}
          </div>
        </div>
        <div
          className="position-relative w-100 z-1"
          style={
            {
              paddingTop: '15%',
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
              data={data}
              series={series}
              scales={scales}
              onReady={onReady}
              onSetSelect={onSetSelect}
              onUpdatePreview={onUpdatePreview}
              onSetCursor={onSetCursor}
              className="w-100 h-100 position-absolute top-0 start-0"
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
          <PlotEvents
            className="plot-legend flex-grow-1"
            indexPlot={indexPlot}
            onCursor={onCursor}
            cursor={cursorTime}
          />
        )}
      </div>
    </div>
  );
}
