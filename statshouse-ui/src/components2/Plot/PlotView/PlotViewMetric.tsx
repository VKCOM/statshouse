import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { type PlotViewProps } from './PlotView';
import { black, grey, greyDark } from 'view/palette';
import { buildThresholdList, useIntersectionObserver, useStateToRef, useUPlotPluginHooks } from 'hooks';
import cn from 'classnames';
import { useStatsHouseShallow } from 'store2';
import { yAxisSize } from 'common/settings';
import { PlotHealsStatus } from './PlotHealsStatus';
import { PlotHeader } from './PlotHeader';
import { PlotSubMenu } from './PlotSubMenu';
import { LegendItem, UPlotPluginPortal, UPlotWrapper, UPlotWrapperPropsOpts } from 'components';
import { PlotLegend } from '../PlotLegend';
import { dateRangeFormat } from './dateRangeFormat';
import { dataIdxNearest } from 'common/dataIdxNearest';
import { font, getYAxisSize, xAxisValues, xAxisValuesCompact } from 'common/axisValues';
import { formatByMetricType, getMetricType, splitByMetricType } from 'common/formatByMetricType';
import { METRIC_TYPE } from 'api/enum';
import { useLinkCSV2 } from 'hooks/useLinkCSV2';
import { xRangeStatic } from './xRangeStatic';
import { calcYRange } from 'common/calcYRange';
import { useThemeStore } from 'store';
import css from './style.module.css';

const rightPad = 16;
const threshold = buildThresholdList(1);

// const themeDark = false;
// const xAxisSize = 16;
const unFocusAlfa = 1;
const yLockDefault = { min: 0, max: 0 };
const syncGroup = '1';

export function PlotViewMetric({ className, plotKey }: PlotViewProps) {
  const {
    yLock,
    numSeries,
    error403,
    isEmbed,
    isDashboard,
    metricUnit,
    metricUnitData,
    topInfo,
    data,
    series,
    scales,
    plotWhat,
    plotDataWhat,
    setPlotVisibility,
  } = useStatsHouseShallow(
    ({ plotsData, params: { plots, tabNum }, metricMeta, isEmbed, baseRange, setPlotVisibility }) => {
      const plot = plots[plotKey];
      const plotData = plotsData[plotKey];
      return {
        plotWhat: plot?.what,
        plotDataWhat: plotData?.whats,
        topInfo: plotData?.topInfo,
        yLock: plot?.yLock,
        numSeries: plot?.numSeries ?? 0,
        error403: plotData?.error403 ?? '',
        metricUnit: plot?.metricUnit,
        metricUnitData: plotData?.metricUnit ?? metricMeta[plot?.metricName ?? '']?.metric_type,
        data: plotData?.data,
        series: plotData?.series,
        scales: plotData?.scales,
        isEmbed,
        isDashboard: +tabNum < 0,
        baseRange,
        setPlotVisibility,
      };
    }
  );
  const themeDark = useThemeStore((s) => s.dark);
  const compact = isDashboard || isEmbed;
  const yLockRef = useStateToRef(yLock ?? yLockDefault);
  const getAxisStroke = useCallback(() => (themeDark ? grey : black), [themeDark]);
  //const { indexPlot, compact, yAxisSize, dashboard, className, group, embed } = props;
  //
  //   const selectorParamsPlot = useMemo(() => selectorParamsPlotsByIndex.bind(undefined, indexPlot), [indexPlot]);
  //   const sel = useStore(selectorParamsPlot);
  //   const setSel = useMemo(() => setPlotParams.bind(undefined, indexPlot), [indexPlot]);
  //
  //   const timeRange = useStore(selectorTimeRange);
  //
  //   const baseRange = useStore(selectorBaseRange);
  //
  //   const { live, interval } = useLiveModeStore((s) => s);
  //
  //   const selectorPlotsData = useMemo(() => selectorPlotsDataByIndex.bind(undefined, indexPlot), [indexPlot]);
  //   const {
  //     scales,
  //     series,
  //     seriesShow,
  //     data,
  //     legendNameWidth,
  //     legendValueWidth,
  //     legendMaxDotSpaceWidth,
  //     legendMaxHostWidth,
  //     mappingFloodEvents,
  //     samplingFactorSrc,
  //     samplingFactorAgg,
  //     receiveErrors,
  //     receiveWarnings,
  //     error: lastError,
  //     error403,
  //     topInfo,
  //     nameMetric,
  //     whats,
  //     metricType: metaMetricType,
  //   } = useStore(selectorPlotsData, shallow);
  //
  //   const onYLockChange = useMemo(() => setYLockChange?.bind(undefined, indexPlot), [indexPlot]);
  //
  //   const selectorNumQueries = useMemo(() => selectorNumQueriesPlotByIndex.bind(undefined, indexPlot), [indexPlot]);
  //   const numQueries = useStore(selectorNumQueries);
  //
  //   const selectorPlotMetricsMeta = useMemo(
  //     () => selectorMetricsMetaByName.bind(undefined, sel.metricName ?? ''),
  //     [sel.metricName]
  //   );
  //   const meta = useStore(selectorPlotMetricsMeta);
  const [cursorLock, setCursorLock] = useState(false);

  //
  //   const plotHeals = usePlotHealsStore((s) => s.status[indexPlot]);
  //   const healsInfo = useMemo(() => {
  //     if (plotHeals?.timout && interval < plotHeals.timout) {
  //       return `plot update timeout ${plotHeals.timout} sec`;
  //     }
  //     return undefined;
  //   }, [interval, plotHeals?.timout]);
  //
  const uPlotRef = useRef<uPlot>();
  const [legend, setLegend] = useState<LegendItem[]>([]);

  const [pluginEventOverlay, pluginEventOverlayHooks] = useUPlotPluginHooks();

  //   const metricName = useMemo(
  //     () => (sel.metricName !== promQLMetric ? sel.metricName : nameMetric),
  //     [sel.metricName, nameMetric]
  //   );
  //
  //   const clearLastError = useCallback(() => {
  //     setPlotLastError(indexPlot, '');
  //     removePlotHeals(indexPlot.toString());
  //   }, [indexPlot]);
  //
  //   const reload = useCallback(() => {
  //     setPlotLastError(indexPlot, '');
  //     removePlotHeals(indexPlot.toString());
  //     loadPlot(indexPlot);
  //   }, [indexPlot]);
  //
  //   const resetZoom = useCallback(() => {
  //     setSel(
  //       produce((s) => {
  //         s.yLock = { min: 0, max: 0 };
  //       })
  //     );
  //     setTimeRange(timeRangeAbbrevExpand(baseRange, now()));
  //   }, [setSel, baseRange]);
  //
  const topPad = compact ? 8 : 16;
  const xAxisSize = compact ? 32 : 48;

  //   // all of this so that our "create plot" layout effect does not depend on the time range
  //   const resetZoomRef = useRef(resetZoom);
  //   useEffect(() => {
  //     resetZoomRef.current = resetZoom;
  //   }, [resetZoom]);
  //
  const onSetSelect = useCallback((u: uPlot) => {
    if (u.status === 1) {
      const xMin = u.posToVal(u.select.left, 'x');
      const xMax = u.posToVal(u.select.left + u.select.width, 'x');
      const yMin = u.posToVal(u.select.top + u.select.height, 'y');
      const yMax = u.posToVal(u.select.top, 'y');
      const xOnly = u.select.top === 0;
      if (!xOnly) {
        // setSel(
        //   produce((s) => {
        //     s.yLock = { min: yMin, max: yMax }; // unfortunately will cause a re-scale from the effect
        //   })
        // );
      } else {
        // setLiveMode(false);
        // setTimeRange({ from: Math.floor(xMin), to: Math.ceil(xMax) });
      }
    }
  }, []);

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
    // const sync: uPlot.Cursor.Sync | undefined = group
    //   ? {
    //       key: group,
    //       filters: {
    //         sub(event) {
    //           return event !== 'mouseup' && event !== 'mousedown';
    //         },
    //         pub(event) {
    //           return event !== 'mouseup' && event !== 'mousedown';
    //         },
    //       },
    //     }
    //   : undefined;
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
      plugins: [pluginEventOverlay],
    };
  }, [compact, getAxisStroke, metricType, pluginEventOverlay, themeDark, topPad, xAxisSize, yLockRef]);
  //
  const linkCSV = useLinkCSV2(plotKey);

  const onReady = useCallback((u: uPlot) => {
    if (uPlotRef.current !== u) {
      uPlotRef.current = u;
    }
    // setUPlotWidth(indexPlot, u.bbox.width);
    u.over.onclick = () => {
      // @ts-ignore
      setCursorLock(u.cursor._lock);
    };
    u.over.ondblclick = () => {
      // resetZoomRef.current();
    };
    u.setCursor({ top: -10, left: -10 }, false);
  }, []);

  const onUpdatePreview = useCallback((u: uPlot) => {
    // createPlotPreview(indexPlot, u);
  }, []);

  const [fixHeight, setFixHeight] = useState<number>(0);
  const divOut = useRef<HTMLDivElement>(null);
  const visible = useIntersectionObserver(divOut?.current, threshold, undefined, 0);
  const onMouseOver = useCallback(() => {
    if (divOut.current && !isEmbed) {
      setFixHeight(divOut.current.getBoundingClientRect().height);
    }
  }, [isEmbed]);
  const onMouseOut = useCallback(() => {
    setFixHeight(0);
  }, []);

  const onLegendFocus = useCallback((index: number, focus: boolean) => {
    if ((uPlotRef.current?.cursor as { _lock: boolean })._lock) {
      return;
    }
    uPlotRef.current?.setSeries(index, { focus }, true);
  }, []);
  //
  //   const onLegendShow = useCallback(
  //     (index: number, show: boolean, single: boolean) => {
  //       setPlotShow(indexPlot, index - 1, show, single);
  //     },
  //     [indexPlot]
  //   );
  //   useEffect(() => {
  //     seriesShow.forEach((show, idx) => {
  //       if (uPlotRef.current?.series[idx + 1] && uPlotRef.current?.series[idx + 1].show !== show) {
  //         uPlotRef.current?.setSeries(idx + 1, { show }, true);
  //       }
  //     });
  //   }, [seriesShow]);

  useEffect(() => {
    setPlotVisibility(plotKey, visible > 0);
  }, [plotKey, setPlotVisibility, visible]);

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
          //     '--legend-name-width': `${legendNameWidth}px`,
          //     '--legend-value-width': `${legendValueWidth}px`,
          //     '--legend-max-host-width': `${legendMaxHostWidth}px`,
          //     '--legend-dot-space-width': `${legendMaxDotSpaceWidth}px`,
          height: fixHeight > 0 && isDashboard ? `${fixHeight}px` : undefined,
        } as React.CSSProperties
      }
      onMouseOver={onMouseOver}
      onMouseOut={onMouseOut}
    >
      <div className="plot-view-inner">
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
            <PlotHeader plotKey={plotKey} />
            {!compact && <PlotSubMenu plotKey={plotKey} />}
            {/*{!compact && (*/}
            {/*meta*/}
            {/*  <PlotSubMenu*/}
            {/*    linkCSV={linkCSV}*/}
            {/*    mappingFloodEvents={mappingFloodEvents}*/}
            {/*    timeRange={timeRange}*/}
            {/*    sel={sel}*/}
            {/*    receiveErrors={receiveErrors}*/}
            {/*    receiveWarnings={receiveWarnings}*/}
            {/*    samplingFactorAgg={samplingFactorAgg}*/}
            {/*    samplingFactorSrc={samplingFactorSrc}*/}
            {/*    metricName={metricName}*/}
            {/*  />*/}
            {/*)}*/}
          </div>
        </div>
        <div
          className="position-relative w-100 z-1"
          style={
            {
              paddingTop: '61.8034%',
              // '--plot-padding-top': `${topPad}px`,
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
              className={cn('w-100 h-100 position-absolute top-0 start-0', cursorLock && css.cursorLock)}
              onUpdateLegend={setLegend}
            >
              <UPlotPluginPortal hooks={pluginEventOverlayHooks} zone="over">
                {/*<PlotEventOverlay*/}
                {/*  indexPlot={indexPlot}*/}
                {/*  hooks={pluginEventOverlayHooks}*/}
                {/*  flagHeight={Math.min(topPad, 10)}*/}
                {/*  compact={compact}*/}
                {/*/>*/}
              </UPlotPluginPortal>
            </UPlotWrapper>
          )}
        </div>
        {!error403 && (
          <div className="plot-legend">
            <PlotLegend
            // indexPlot={indexPlot}
            // legend={legend as LegendItem<PlotValues>[]}
            // onLegendShow={onLegendShow}
            // onLegendFocus={onLegendFocus}
            // compact={compact && !(fixHeight > 0 && dashboard)}
            // unit={metricType}
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

export const incrs = [
  1e-16, 2e-16, 2.5e-16, 5e-16, 1e-15, 2e-15, 2.5e-15, 5e-15, 1e-14, 2e-14, 2.5e-14, 5e-14, 1e-13, 2e-13, 2.5e-13,
  5e-13, 1e-12, 2e-12, 2.5e-12, 5e-12, 1e-11, 2e-11, 2.5e-11, 5e-11, 1e-10, 2e-10, 2.5e-10, 5e-10, 1e-9, 2e-9, 2.5e-9,
  5e-9, 1e-8, 2e-8, 2.5e-8, 5e-8, 1e-7, 2e-7, 2.5e-7, 5e-7, 0.000001, 0.000002, 0.0000025, 0.000005, 0.00001, 0.00002,
  0.000025, 0.00005, 0.0001, 0.0002, 0.00025, 0.0005, 0.001, 0.002, 0.0025, 0.005, 0.01, 0.02, 0.025, 0.05, 0.1, 0.2,
  0.25, 0.5, 1, 2, 2.5, 5, 10, 20, 25, 50, 100, 200, 250, 500, 1000, 2000, 2500, 5000, 10000, 20000, 25000, 50000,
  100000, 200000, 250000, 500000, 1000000, 2000000, 2500000, 5000000, 10000000, 20000000, 25000000, 50000000, 100000000,
  200000000, 250000000, 500000000, 1000000000, 2000000000, 2500000000, 5000000000, 10000000000, 20000000000,
  25000000000, 50000000000, 100000000000, 200000000000, 250000000000, 500000000000, 1000000000000, 2000000000000,
  2500000000000, 5000000000000, 10000000000000, 20000000000000, 25000000000000, 50000000000000, 100000000000000,
  200000000000000, 250000000000000, 500000000000000, 1000000000000000, 2000000000000000, 2500000000000000,
  5000000000000000, 10e15, 25e15, 50e15, 10e16, 25e16, 50e16, 10e17, 25e17, 50e17, 10e18, 25e18, 50e18, 10e19, 25e19,
  50e19, 10e20, 25e20, 50e20, 10e21, 25e21, 50e21, 10e22, 25e22, 50e22, 10e23, 25e23, 50e23, 10e24, 25e24, 50e24, 10e25,
  25e25, 50e25, 10e26, 25e26, 50e26, 10e27, 25e27, 50e27, 10e28, 25e28, 50e28, 10e29, 25e29, 50e29, 10e30, 25e30, 50e30,
  10e31, 25e31, 50e31,
];
