import { createStore } from '../createStore';
import {
  type PlotKey,
  PlotParams,
  promQLMetric,
  QueryParams,
  TimeRange,
  urlEncodePlotFilters,
  useUrlStore,
} from '../urlStore';
import uPlot from 'uplot';
import { apiQueryFetch, ApiQueryGet, SeriesResponse } from '../../api/query';
import {
  GET_PARAMS,
  isQueryWhat,
  METRIC_TYPE,
  METRIC_VALUE_BACKEND_VERSION,
  PLOT_TYPE,
  QUERY_WHAT,
  QueryWhat,
} from '../../api/enum';
import { PlotsInfoStore, usePlotsInfoStore } from './plotsInfoStore';
import { produce } from 'immer';
import { mergeLeft } from '../../common/helpers';
import { SelectOptionProps, UPlotWrapperPropsScales } from '../../components';
import { filterPoints } from '../../common/filterPoints';
import { metaToBaseLabel, metaToLabel, querySeriesMeta } from '../../view/api';
import { rgba, selectColor } from '../../view/palette';
import { formatLegendValue, formatPercent, timeShiftToDash } from '../../view/utils';
import { loadMetricMeta } from '../metricsStore';
import { TopInfo } from '../../store';
import { pxPerChar } from '../../common/settings';
import { stackData } from '../../common/stackData';
import { calcYRange2 } from '../../common/calcYRange';
import { dequal } from 'dequal/lite';

export function getEmptyPlotData(): PlotData {
  return {
    data: [],
    dataView: [],
    bands: [],
    series: [],
    seriesShow: [],
    scales: {},
    promQL: '',
    nameMetric: '',
    whats: [],
    metricUnit: METRIC_TYPE.none,
    error: '',
    error403: '',
    errorSkipCount: 0,
    seriesTimeShift: [],
    // lastPlotParams?: PlotParams;
    // lastTimeRange?: TimeRange;
    // lastTimeShifts?: number[];
    // lastQuerySeriesMeta?: querySeriesMeta[];
    receiveErrors: 0,
    receiveWarnings: 0,
    samplingFactorSrc: 0,
    samplingFactorAgg: 0,
    mappingFloodEvents: 0,
    legendValueWidth: 0,
    legendMaxDotSpaceWidth: 0,
    legendNameWidth: 0,
    legendPercentWidth: 0,
    legendMaxHostWidth: 0,
    legendMaxHostPercentWidth: 0,
    // topInfo?: TopInfo;
    maxHostLists: [],
    promqltestfailed: false,
    promqlExpand: false,
  };
}

export type PlotValues = {
  rawValue: number | null;
  value: string;
  metricName: string;
  label: string;
  baseLabel: string;
  timeShift: number;
  max_host: string;
  total: number;
  percent: string;
  max_host_percent: string;
  top_max_host: string;
  top_max_host_percent: string;
};

export type PlotData = {
  data: uPlot.AlignedData;
  dataView: uPlot.AlignedData;
  bands?: uPlot.Band[];
  series: uPlot.Series[];
  seriesShow: boolean[];
  scales: UPlotWrapperPropsScales;
  promQL: string;
  nameMetric: string;
  whats: QueryWhat[];
  metricUnit: string;
  error: string;
  error403?: string;
  errorSkipCount: number;
  seriesTimeShift: number[];
  lastPlotParams?: PlotParams;
  lastTimeRange?: TimeRange;
  lastTimeShifts?: number[];
  lastQuerySeriesMeta?: querySeriesMeta[];
  receiveErrors: number;
  receiveWarnings: number;
  samplingFactorSrc: number;
  samplingFactorAgg: number;
  mappingFloodEvents: number;
  legendValueWidth: number;
  legendMaxDotSpaceWidth: number;
  legendNameWidth: number;
  legendPercentWidth: number;
  legendMaxHostWidth: number;
  legendMaxHostPercentWidth: number;
  topInfo?: TopInfo;
  maxHostLists: SelectOptionProps[][];
  promqltestfailed?: boolean;
  promqlExpand: boolean;
};

export type PlotsDataStore = {
  plotsData: Partial<Record<PlotKey, PlotData>>;
};

export const usePlotsDataStore = createStore<PlotsDataStore>(() => ({ plotsData: {} }));

export async function loadPlotData(plotKey: PlotKey, params: QueryParams, priority?: number) {
  const plot = params.plots[plotKey];
  if (!plot) {
    return;
  }
  const fetchBadges = true;
  const width = plot.customAgg === -1 ? `500` : plot.customAgg === 0 ? `2000` : `${plot.customAgg}s`;
  const urlParams: ApiQueryGet = {
    [GET_PARAMS.numResults]: plot.numSeries.toString(),
    [GET_PARAMS.metricWhat]: plot.what.slice(),
    [GET_PARAMS.toTime]: params.timeRange.to.toString(),
    [GET_PARAMS.fromTime]: params.timeRange.from.toString(),
    [GET_PARAMS.width]: width,
    [GET_PARAMS.version]: plot.useV2 ? METRIC_VALUE_BACKEND_VERSION.v2 : METRIC_VALUE_BACKEND_VERSION.v1,
    [GET_PARAMS.metricFilter]: urlEncodePlotFilters('', plot.filterIn, plot.filterNotIn).map(([, v]) => v),
    [GET_PARAMS.metricGroupBy]: plot.groupBy,
    // [GET_PARAMS.metricAgg]: plot.customAgg.toString(),
    [GET_PARAMS.metricTimeShifts]: params.timeShifts.map((t) => t.toString()),
    [GET_PARAMS.excessPoints]: '1',
    [GET_PARAMS.metricVerbose]: fetchBadges ? '1' : '0',
  };
  if (plot.metricName && plot.metricName !== promQLMetric) {
    urlParams[GET_PARAMS.metricName] = plot.metricName;
  }
  if (plot.promQL || plot.metricName === promQLMetric) {
    urlParams[GET_PARAMS.metricPromQL] = plot.promQL;
  }
  if (plot.maxHost) {
    urlParams[GET_PARAMS.metricMaxHost] = '1';
  }
  // if (fetchBadges) {
  //   urlParams[GET_PARAMS.metricVerbose] = '1';
  // }
  if (priority) {
    urlParams[GET_PARAMS.priority] = priority.toString();
  }
  loadMetricMeta(plot.metricName).then();
  const { response, error, status } = await apiQueryFetch(urlParams, `loadPlotData_${plotKey}`);

  if (error) {
    if (status === 403) {
    }
  }
  if (response) {
    const data = normalizePlotData(response.data, plot, params);
    usePlotsDataStore.setState(
      produce<PlotsDataStore>((state) => {
        state.plotsData[plotKey] = mergeLeft(state.plotsData[plotKey], data);
      })
    );
  }
}

export function normalizePlotData(response: SeriesResponse, plot: PlotParams, params: QueryParams): PlotData {
  const width = 2000;
  const { timeRange, timeShifts } = params;
  return produce<PlotData>(usePlotsDataStore.getState().plotsData[plot.id] ?? getEmptyPlotData(), (plotData) => {
    const {
      lastPlotParams: currentPrevLastPlotParams,
      seriesShow: currentPrevSeriesShow,
      series: currentPrevSeries,
    } = plotData;
    plotData.promqltestfailed = response.promqltestfailed;

    const uniqueWhat: Set<QueryWhat> = new Set();
    const uniqueName = new Set();
    const uniqueMetricType: Set<string> = new Set();
    let series_meta = [...response.series.series_meta];
    let series_data = [...response.series.series_data];
    const totalLineId = plot.totalLine ? series_meta.length : null;
    const totalLineLabel = 'Total';
    if (plot.totalLine) {
      const totalLineData = response.series.time.map((time, idx) =>
        series_data.reduce((res, d) => res + (d[idx] ?? 0), 0)
      );
      series_meta.push({
        name: totalLineLabel,
        time_shift: 0,
        tags: { '0': { value: totalLineLabel } },
        max_hosts: null,
        what: QUERY_WHAT.sum,
        total: 0,
        color: '#333333',
      });
      series_data.push(totalLineData);
    }
    if (plot.type === PLOT_TYPE.Event) {
      series_meta = [];
      series_data = [];
      const colorIndex = new Map<string, number>();
      response.series.series_meta.forEach((series, indexSeries) => {
        const indexColor = colorIndex.get(series.color);
        if (series.color && indexColor != null) {
          response.series.series_data[indexSeries].forEach((value, indexValue) => {
            if (value != null) {
              series_data[indexColor][indexValue] = (series_data[indexColor][indexValue] ?? 0) + value;
            }
          });
        } else {
          const index = series_meta.push(series) - 1;
          series_data.push([...response.series.series_data[indexSeries]]);
          colorIndex.set(series.color, index);
        }
      });
    }

    for (const meta of series_meta) {
      if (isQueryWhat(meta.what)) {
        uniqueWhat.add(meta.what);
      }
      meta.name && uniqueName.add(meta.name);
      if (meta.metric_type) {
        uniqueMetricType.add(meta.metric_type);
      }
    }
    // const currentPrevState = getState();

    // const currentPrevSeries = getState().plotsData[index].series.map((s) => ({ ...s, values: undefined }));
    if (uniqueName.size === 0 && currentPrevLastPlotParams && currentPrevLastPlotParams.metricName !== promQLMetric) {
      uniqueName.add(currentPrevLastPlotParams.metricName);
    }
    plotData.metricUnit = uniqueMetricType.size === 1 ? [...uniqueMetricType.keys()][0] : METRIC_TYPE.none;

    const maxLabelLength = Math.max(
      'Time'.length,
      ...series_meta.map((meta) => {
        const label = metaToLabel(meta, uniqueWhat.size);
        return label.length;
      })
    );
    plotData.legendNameWidth = (series_meta.length ?? 0) > 5 ? maxLabelLength * pxPerChar : 1_000_000;

    plotData.legendMaxHostWidth = 0;
    plotData.legendMaxHostPercentWidth = 0;
    const localData: uPlot.AlignedData = [response.series.time, ...series_data];
    plotData.dataView = plotData.data = localData;

    if (currentPrevLastPlotParams?.type === PLOT_TYPE.Event) {
      const stacked = stackData(plotData.data);
      plotData.dataView = stacked.data;
      plotData.bands = stacked.bands;
    }
    const usedDashes = {};
    const usedBaseColors = {};
    const baseColors: Record<string, string> = {};
    // let changeColor = false;
    // let changeType = currentPrevLastPlotParams?.type !== plot.type;
    // const changeView =
    //   currentPrevLastPlotParams?.totalLine !== plot.totalLine ||
    //   currentPrevLastPlotParams?.filledGraph !== plot.filledGraph;
    const widthLine =
      (width ?? 0) > response.series.time.length
        ? devicePixelRatio > 1
          ? 2 / devicePixelRatio
          : 1
        : 1 / devicePixelRatio;

    const topInfoCounts: Record<string, number> = {};
    const topInfoTotals: Record<string, number> = {};
    plotData.topInfo = undefined;
    const maxHostLists: SelectOptionProps[][] = new Array(series_meta.length).fill([]);
    plotData.maxHostLists = maxHostLists;
    const oneGraph = series_meta.filter((s) => s.time_shift === 0).length <= 1;
    const seriesShow = new Array(series_meta.length).fill(true);
    plotData.seriesTimeShift = [];
    plotData.series = series_meta.map((meta, indexMeta): uPlot.Series => {
      const timeShift = meta.time_shift !== 0;
      plotData.seriesTimeShift[indexMeta] = meta.time_shift;
      const label = totalLineId !== indexMeta ? metaToLabel(meta, uniqueWhat.size) : totalLineLabel;
      const baseLabel = totalLineId !== indexMeta ? metaToBaseLabel(meta, uniqueWhat.size) : totalLineLabel;
      const isValue = baseLabel.indexOf('Value') === 0;
      const prefColor = '9'; // it`s magic prefix
      const metricName = isValue ? `${meta.name || (plot.metricName !== promQLMetric ? plot.metricName : '')}: ` : '';
      const colorKey = `${prefColor}${metricName}${oneGraph ? label : baseLabel}`;
      const baseColor = meta.color ?? baseColors[colorKey] ?? selectColor(colorKey, usedBaseColors);
      baseColors[colorKey] = baseColor;
      // if (baseColor !== currentPrevSeries[indexMeta]?.stroke) {
      //   changeColor = true;
      // }
      if (meta.max_hosts) {
        const max_hosts_l = meta.max_hosts
          .map((host) => host.length * pxPerChar * 1.25 + 65)
          .filter(Boolean)
          .sort();
        const full = max_hosts_l[0] ?? 0;
        const p75 = max_hosts_l[Math.floor(max_hosts_l.length * 0.25)] ?? 0;
        plotData.legendMaxHostWidth = Math.max(plotData.legendMaxHostWidth, full - p75 > 20 ? p75 : full);
      }
      const max_host_map =
        meta.max_hosts?.reduce(
          (res, host) => {
            if (host) {
              res[host] = (res[host] ?? 0) + 1;
            }
            return res;
          },
          {} as Record<string, number>
        ) ?? {};
      const max_host_total = meta.max_hosts?.filter(Boolean).length ?? 1;
      seriesShow[indexMeta] = currentPrevSeries[indexMeta]?.label === label ? currentPrevSeriesShow[indexMeta] : true;
      maxHostLists[indexMeta] = Object.entries(max_host_map)
        .sort(([k, a], [n, b]) => (a > b ? -1 : a < b ? 1 : k > n ? 1 : k < n ? -1 : 0))
        .map(([host, count]) => {
          const percent = formatPercent(count / max_host_total);
          return {
            value: host,
            title: `${host}: ${percent}`,
            name: `${host}: ${percent}`,
            html: `<div class="d-flex"><div class="flex-grow-1 me-2 overflow-hidden text-nowrap">${host}</div><div class="text-end">${percent}</div></div>`,
          };
        });
      if (totalLineId !== indexMeta) {
        const key = `${meta.what}|${meta.time_shift}`;
        topInfoCounts[key] = (topInfoCounts[key] ?? 0) + 1;
        topInfoTotals[key] = meta.total;
      }
      const paths =
        plot.type === PLOT_TYPE.Event
          ? uPlot.paths.bars!({ size: [0.7], gap: 0, align: 1 })
          : uPlot.paths.stepped!({
              align: 1,
            });
      return {
        show: seriesShow[indexMeta] ?? true,
        auto: false, // we control the scaling manually
        label,
        stroke: baseColor,
        width: widthLine,
        dash: timeShift ? timeShiftToDash(meta.time_shift, usedDashes) : undefined,
        fill: totalLineId !== indexMeta && plot.filledGraph ? rgba(baseColor, timeShift ? 0.1 : 0.15) : undefined,
        points:
          plot.type === PLOT_TYPE.Event
            ? { show: false, size: 0 }
            : {
                filter: filterPoints,
                size: 5,
              },
        paths,
        values(u, seriesIdx, idx): PlotValues {
          if (idx === null) {
            return {
              metricName: '',
              rawValue: null,
              value: '',
              label: '',
              baseLabel: '',
              timeShift: 0,
              max_host: '',
              total: 0,
              percent: '',
              max_host_percent: '',
              top_max_host: '',
              top_max_host_percent: '',
            };
          }
          const rawValue = localData[seriesIdx]?.[idx] ?? null;
          let total = 0;
          for (let i = 1; i < u.series.length; i++) {
            const v = localData[i]?.[idx];
            if (v !== null && v !== undefined && i - 1 !== totalLineId) {
              total += v;
            }
          }
          const value = formatLegendValue(rawValue);
          const max_host = meta.max_hosts !== null && idx < meta.max_hosts.length ? meta.max_hosts[idx] : '';

          const max_host_percent =
            meta.max_hosts !== null && max_host_map && meta.max_hosts[idx]
              ? formatPercent((max_host_map[meta.max_hosts[idx]] ?? 0) / max_host_total)
              : '';
          const percent = rawValue !== null ? formatPercent(rawValue / total) : '';
          return {
            metricName,
            rawValue,
            value,
            label,
            baseLabel,
            timeShift: meta.time_shift,
            max_host,
            total,
            percent: totalLineId !== indexMeta ? percent : '100%',
            max_host_percent,
            top_max_host: maxHostLists[indexMeta]?.[0]?.value ?? '',
            top_max_host_percent: maxHostLists[indexMeta]?.[0]?.title ?? '',
          };
        },
      };
    });
    if (!dequal(plotData.seriesShow, seriesShow)) {
      plotData.seriesShow = seriesShow;
    }
    const topInfoTop = {
      min: Math.min(...Object.values(topInfoCounts)),
      max: Math.max(...Object.values(topInfoCounts)),
    };
    const topInfoTotal = {
      min: Math.min(...Object.values(topInfoTotals)),
      max: Math.max(...Object.values(topInfoTotals)),
    };
    const topInfoFunc = currentPrevLastPlotParams?.what.length ?? 0;
    const topInfoShifts = timeShifts.length;
    const info: string[] = [];
    //
    if (topInfoTop.min !== topInfoTotal.min && topInfoTop.max !== topInfoTotal.max) {
      if (topInfoFunc > 1) {
        info.push(`${topInfoFunc} functions`);
      }
      if (topInfoShifts > 0) {
        info.push(`${topInfoShifts} time-shift${topInfoShifts > 1 ? 's' : ''}`);
      }
      plotData.topInfo = {
        top: topInfoTop.max === topInfoTop.min ? topInfoTop.max.toString() : `${topInfoTop.min}-${topInfoTop.max}`,
        total:
          topInfoTotal.max === topInfoTotal.min
            ? topInfoTotal.max.toString()
            : `${topInfoTotal.min}-${topInfoTotal.max}`,
        info: info.length ? ` (${info.join(',')})` : '',
      };
    }

    plotData.scales.x = { min: timeRange.from + timeRange.to, max: timeRange.to };
    if (plot.yLock.min !== 0 || plot.yLock.max !== 0) {
      plotData.scales.y = { ...plot.yLock };
    } else {
      plotData.scales.y = { min: 0, max: 0 };
    }
    plotData.promQL = response.promql;

    const maxLengthValue = plotData.series.reduce(
      (res, s, indexSeries) => {
        if (s.show) {
          const v: null | number =
            (plotData.data[indexSeries + 1] as (number | null)[] | undefined)?.reduce(
              (res2, d) => {
                if (d && (res2?.toString().length ?? 0) < d.toString().length) {
                  return d;
                }
                return res2;
              },
              null as null | number
            ) ?? null;
          if (v && (v.toString().length ?? 0) > (res?.toString().length ?? 0)) {
            return v;
          }
        }
        return res;
      },
      null as null | number
    );
    const [yMinAll, yMaxAll] = calcYRange2(plotData.series, plotData.data, false);
    const legendExampleValue = Math.max(Math.abs(Math.floor(yMinAll) - 0.001), Math.abs(Math.ceil(yMaxAll) + 0.001));
    plotData.legendValueWidth = (formatLegendValue(legendExampleValue).length + 2) * pxPerChar; // +2 - focus marker

    plotData.legendMaxDotSpaceWidth =
      Math.max(4, (formatLegendValue(maxLengthValue).split('.', 2)[1]?.length ?? 0) + 2) * pxPerChar;
    plotData.legendPercentWidth = (4 + 2) * pxPerChar; // +2 - focus marker

    // setState((state) => {
    //   delete state.plotsDataAbortController[index];
    //   const noUpdateData = dequal(stacked?.response || response, state.plotsData[index]?.stacked || state.plotsData[index]?.response);
    //   if (resp.metric != null && !dequal(state.metricsMeta[resp.metric.name], resp.metric)) {
    //     state.metricsMeta[resp.metric.name] = resp.metric;
    //   }
    //   const whats = uniqueName.size === 1 ? [...uniqueWhat.keys()] : [];
    //   state.plotsData[index] = {
    //     nameMetric: uniqueName.size === 1 ? ([...uniqueName.keys()][0] as string) : '',
    //     whats: dequal(whats, state.plotsData[index]?.whats) ? state.plotsData[index]?.whats : whats,
    //     metricType,
    //     error: usePlotHealsStore.getState().status[index]?.status ? '' : state.plotsData[index]?.error,
    //     errorSkipCount: 0,
    //     response: noUpdateData ? state.plotsData[index]?.response : response,
    //     stacked: noUpdateData ? state.plotsData[index]?.stacked : stacked?.response,
    //     bands: dequal(state.plotsData[index]?.bands, stacked?.bands) ? state.plotsData[index]?.bands : stacked?.bands,
    //     series:
    //       dequal(resp.series.series_meta, state.plotsData[index]?.lastQuerySeriesMeta) &&
    //       !changeColor &&
    //       !changeType &&
    //       !changeView
    //         ? state.plotsData[index]?.series
    //         : series,
    //     seriesTimeShift: dequal(seriesTimeShift, state.plotsData[index].seriesTimeShift)
    //       ? state.plotsData[index].seriesTimeShift
    //       : seriesTimeShift,
    //     seriesShow: dequal(seriesShow, state.plotsData[index]?.seriesShow)
    //       ? state.plotsData[index]?.seriesShow
    //       : seriesShow,
    //     scales: dequal(scales, state.plotsData[index]?.scales) ? state.plotsData[index]?.scales : scales,
    //     receiveErrors: resp.receive_errors,
    //     receiveWarnings: resp.receive_warnings,
    //     samplingFactorSrc: resp.sampling_factor_src,
    //     samplingFactorAgg: resp.sampling_factor_agg,
    //     mappingFloodEvents: resp.mapping_errors,
    //     legendValueWidth,
    //     legendMaxDotSpaceWidth,
    //     legendNameWidth,
    //     legendPercentWidth,
    //     legendMaxHostWidth,
    //     legendMaxHostPercentWidth,
    //     lastPlotParams,
    //     lastQuerySeriesMeta: [...resp.series.series_meta],
    //     lastTimeRange: getState().timeRange,
    //     lastTimeShifts: getState().params.timeShifts,
    //     topInfo,
    //     maxHostLists,
    //     promqltestfailed,
    //     promQL: resp.promql ?? '',
    //   };
    // });
    // addStatus(index.toString(), true);
  });
}

export function plotsDataStoreSubscribe(state: PlotsInfoStore, prevState: PlotsInfoStore) {
  const urlStore = useUrlStore.getState();
  state.viewOrderPlot.forEach((plotKey) => {
    loadPlotData(plotKey, urlStore.params).then();
  });
}

usePlotsInfoStore.subscribe(plotsDataStoreSubscribe);

export function togglePromqlExpand(plotKey: PlotKey, status?: boolean) {
  usePlotsDataStore.setState(
    produce<PlotsDataStore>((s) => {
      const pd = s.plotsData[plotKey];
      if (pd) {
        pd.promqlExpand = status ?? !pd.promqlExpand;
      }
    })
  );
}
