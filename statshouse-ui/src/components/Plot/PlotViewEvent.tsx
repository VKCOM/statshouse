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
import { Button, UPlotWrapper, UPlotWrapperPropsOpts } from '../index';
import { now, promQLMetric, timeRangeAbbrevExpand } from '../../view/utils';
import { black, grey, greyDark } from '../../view/palette';
import { produce } from 'immer';
import {
  selectorBaseRange,
  selectorEventsByIndex,
  selectorLiveMode,
  selectorLoadEvents,
  selectorMetricsMetaByName,
  selectorNumQueriesPlotByIndex,
  selectorParamsPlotsByIndex,
  selectorPlotsDataByIndex,
  selectorTimeRange,
  useStore,
  useThemeStore,
} from '../../store';
import { font, getYAxisSize, xAxisValues, xAxisValuesCompact } from '../../common/axisValues';
import cn from 'classnames';
import { PlotEvents } from './PlotEvents';
import { buildThresholdList, useIntersectionObserver, useUPlotPluginHooks } from '../../hooks';
import { UPlotPluginPortal } from '../UPlotWrapper';
import { dataIdxNearest } from '../../common/dataIdxNearest';
import { ReactComponent as SVGArrowCounterclockwise } from 'bootstrap-icons/icons/arrow-counterclockwise.svg';
import { setPlotVisibility } from '../../store/plot/plotVisibilityStore';
import { createPlotPreview } from '../../store/plot/plotPreview';
import { shallow } from 'zustand/shallow';
import { formatByMetricType, getMetricType, splitByMetricType } from '../../common/formatByMetricType';
import { METRIC_TYPE } from '../../api/enum';
import css from './style.module.css';
import { useLinkCSV } from '../../hooks/useLinkCSV';

const unFocusAlfa = 1;
const rightPad = 16;

const threshold = buildThresholdList(1);

function xRangeStatic(u: uPlot, dataMin: number | null, dataMax: number | null): [number, number] {
  if (dataMin === null || dataMax === null) {
    const t = now();
    return [t - 3600, t];
  }
  return [dataMin, dataMax];
}

const { setPlotParams, setTimeRange, setLiveMode, setYLockChange, setPlotLastError, setUPlotWidth, loadPlot } =
  useStore.getState();

export function PlotViewEvent(props: {
  indexPlot: number;
  className?: string;
  dashboard?: boolean;
  compact: boolean;
  yAxisSize: number;
  group?: string;
  embed?: boolean;
}) {
  const { indexPlot, compact, yAxisSize, dashboard, className, group, embed } = props;
  const selectorParamsPlot = useMemo(() => selectorParamsPlotsByIndex.bind(undefined, indexPlot), [indexPlot]);
  const sel = useStore(selectorParamsPlot);
  const setSel = useMemo(() => setPlotParams.bind(undefined, indexPlot), [indexPlot]);
  const selectorEvent = useMemo(() => selectorEventsByIndex.bind(undefined, indexPlot), [indexPlot]);
  const plotEvent = useStore(selectorEvent);
  const [pluginTimeWindow, pluginTimeWindowHooks] = useUPlotPluginHooks();

  const timeRange = useStore(selectorTimeRange);

  const baseRange = useStore(selectorBaseRange);

  const live = useStore(selectorLiveMode);

  const selectorPlotsData = useMemo(() => selectorPlotsDataByIndex.bind(undefined, indexPlot), [indexPlot]);
  const {
    scales,
    series,
    seriesShow,
    stacked,
    legendNameWidth,
    legendValueWidth,
    legendMaxDotSpaceWidth,
    legendMaxHostWidth,
    mappingFloodEvents,
    samplingFactorSrc,
    samplingFactorAgg,
    receiveErrors,
    receiveWarnings,
    error: lastError,
    error403,
    nameMetric,
    whats,
    metricType: metaMetricType,
  } = useStore(selectorPlotsData, shallow);

  const onYLockChange = useMemo(() => setYLockChange?.bind(undefined, indexPlot), [indexPlot]);

  const selectorNumQueries = useMemo(() => selectorNumQueriesPlotByIndex.bind(undefined, indexPlot), [indexPlot]);
  const numQueries = useStore(selectorNumQueries);

  const selectorPlotMetricsMeta = useMemo(
    () => selectorMetricsMetaByName.bind(undefined, sel.metricName ?? ''),
    [sel.metricName]
  );
  const meta = useStore(selectorPlotMetricsMeta);

  const [cursorLock, setCursorLock] = useState(false);

  const loadEvent = useStore(selectorLoadEvents);

  const themeDark = useThemeStore((s) => s.dark);

  const uPlotRef = useRef<uPlot>();

  const metricName = useMemo(
    () => (sel.metricName !== promQLMetric ? sel.metricName : nameMetric),
    [sel.metricName, nameMetric]
  );

  const clearLastError = useCallback(() => {
    setPlotLastError(indexPlot, '');
  }, [indexPlot]);

  const reload = useCallback(() => {
    setPlotLastError(indexPlot, '');
    loadPlot(indexPlot);
  }, [indexPlot]);

  const resetZoom = useCallback(() => {
    setSel(
      produce((s) => {
        s.yLock = { min: 0, max: 0 };
      })
    );
    setTimeRange(timeRangeAbbrevExpand(baseRange, now()));
  }, [setSel, baseRange]);

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
          setLiveMode(false);
          setTimeRange({ from: Math.floor(xMin), to: Math.ceil(xMax) });
        }
      }
    },
    [setSel]
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
    const metricType = getMetricType(whats?.length ? whats : sel.what, metaMetricType || meta?.metric_type);
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
  }, [
    compact,
    getAxisStroke,
    group,
    meta?.metric_type,
    metaMetricType,
    pluginTimeWindow,
    sel.what,
    themeDark,
    topPad,
    whats,
    xAxisSize,
    yAxisSize,
  ]);

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

  const linkCSV = useLinkCSV(indexPlot);

  const onReady = useCallback(
    (u: uPlot) => {
      if (uPlotRef.current !== u) {
        uPlotRef.current = u;
      }
      setUPlotWidth(indexPlot, u.bbox.width);
      u.over.ondblclick = () => {
        resetZoomRef.current();
      };
      u.over.onclick = () => {
        loadEvent(indexPlot, undefined, false, u.data[0]?.[u.cursor.idx ?? 0]).catch(() => undefined);
        // @ts-ignore
        setCursorLock(u.cursor._lock);
      };
      u.setCursor({ top: -10, left: -10 }, false);
    },
    [indexPlot, loadEvent]
  );

  const onUpdatePreview = useCallback(
    (u: uPlot) => {
      createPlotPreview(indexPlot, u);
    },
    [indexPlot]
  );

  const [fixHeight, setFixHeight] = useState<number>(0);
  const divOut = useRef<HTMLDivElement>(null);
  const visible = useIntersectionObserver(divOut?.current, threshold, undefined, 0);
  const onMouseOver = useCallback(() => {
    if (divOut.current && !embed) {
      setFixHeight(divOut.current.getBoundingClientRect().height);
    }
  }, [embed]);
  const onMouseOut = useCallback(() => {
    setFixHeight(0);
  }, []);

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
    seriesShow.forEach((show, idx) => {
      if (uPlotRef.current?.series[idx + 1]?.show !== show) {
        uPlotRef.current?.setSeries(idx + 1, { show }, true);
      }
    });
  }, [seriesShow]);

  useEffect(() => {
    setPlotVisibility(indexPlot, visible > 0);
  }, [indexPlot, visible]);

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
                <Button type="button" className="btn" aria-label="Reload" onClick={reload}>
                  <SVGArrowCounterclockwise />
                </Button>
                <small className="overflow-force-wrap font-monospace">{lastError}</small>
                <Button type="button" className="btn-close" aria-label="Close" onClick={clearLastError} />
              </div>
            )}
            <PlotHeader
              indexPlot={indexPlot}
              setParams={setSel}
              setLive={setLiveMode}
              meta={meta}
              live={live}
              yLock={sel.yLock}
              setTimeRange={setTimeRange}
              onResetZoom={resetZoom}
              onYLockChange={onYLockChange}
              dashboard={dashboard}
              compact={compact}
              embed={embed}
            />
            {!compact && (
              /*meta*/
              <PlotSubMenu
                linkCSV={linkCSV}
                mappingFloodEvents={mappingFloodEvents}
                timeRange={timeRange}
                sel={sel}
                receiveErrors={receiveErrors}
                receiveWarnings={receiveWarnings}
                samplingFactorAgg={samplingFactorAgg}
                samplingFactorSrc={samplingFactorSrc}
                metricName={metricName}
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
              data={stacked}
              series={series}
              scales={scales}
              onReady={onReady}
              onSetSelect={onSetSelect}
              onUpdatePreview={onUpdatePreview}
              onSetCursor={onSetCursor}
              className={cn('w-100 h-100 position-absolute top-0 start-0', cursorLock && css.cursorLock)}
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
