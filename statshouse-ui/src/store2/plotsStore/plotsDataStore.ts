import { createStore } from '../createStore';
import { type PlotKey, PlotParams, promQLMetric, TimeRange, urlEncodePlotFilters, useUrlStore } from '../urlStore';
import uPlot from 'uplot';
import { apiQueryFetch, ApiQueryGet, SeriesResponse } from '../../api/query';
import {
  GET_PARAMS,
  isQueryWhat,
  METRIC_VALUE_BACKEND_VERSION,
  PLOT_TYPE,
  QUERY_WHAT,
  QueryWhat,
} from '../../api/enum';
import { PlotsInfoStore, usePlotsInfoStore } from './plotsInfoStore';
import { produce } from 'immer';
import { mergeLeft } from '../../common/helpers';
import { UPlotWrapperPropsScales } from '../../components';
import { filterPoints } from '../../common/filterPoints';
import { metaToBaseLabel, metaToLabel } from '../../view/api';
import { rgba, selectColor } from '../../view/palette';
import { timeShiftToDash } from '../../view/utils';

export function getEmptyPlotData(): PlotData {
  return {
    data: [],
    dataView: [],
    bands: [],
    series: [],
    seriesShow: [],
    scales: {},
  };
}

export type PlotData = {
  data: uPlot.AlignedData;
  dataView: uPlot.AlignedData;
  bands?: uPlot.Band[];
  series: uPlot.Series[];
  // seriesTimeShift: number[];
  seriesShow: boolean[];
  scales: UPlotWrapperPropsScales;
};

export type PlotsDataStore = {
  plotsData: Partial<Record<PlotKey, PlotData>>;
};

export const usePlotsDataStore = createStore<PlotsDataStore>(() => ({ plotsData: {} }));

export async function loadPlotData(plotKey: PlotKey, plot: PlotParams, timeRange: TimeRange, priority?: number) {
  const fetchBadges = true;
  // urlEncodePlotFilters('',plot.filterIn,plot.filterNotIn).map(([k,v])=>v)
  const params: ApiQueryGet = {
    [GET_PARAMS.numResults]: plot.numSeries.toString(),
    [GET_PARAMS.metricWhat]: plot.what.slice(),
    [GET_PARAMS.toTime]: timeRange.to.toString(),
    [GET_PARAMS.fromTime]: timeRange.from.toString(),
    [GET_PARAMS.width]: '2000',
    [GET_PARAMS.version]: plot.useV2 ? METRIC_VALUE_BACKEND_VERSION.v2 : METRIC_VALUE_BACKEND_VERSION.v1,
    [GET_PARAMS.metricFilter]: urlEncodePlotFilters('', plot.filterIn, plot.filterNotIn).map(([, v]) => v),
    [GET_PARAMS.metricGroupBy]: plot.groupBy,
    [GET_PARAMS.metricAgg]: plot.customAgg.toString(),
    [GET_PARAMS.metricTimeShifts]: plot.timeShifts.map(toString),
    [GET_PARAMS.excessPoints]: '1',
  };
  if (plot.metricName && plot.metricName !== promQLMetric) {
    params[GET_PARAMS.metricName] = plot.metricName;
  }
  if (plot.promQL || plot.metricName === promQLMetric) {
    params[GET_PARAMS.metricPromQL] = plot.promQL;
  }
  if (plot.maxHost) {
    params[GET_PARAMS.metricMaxHost] = '1';
  }
  if (fetchBadges) {
    params[GET_PARAMS.metricVerbose] = '1';
  }
  if (priority) {
    params[GET_PARAMS.priority] = priority.toString();
  }

  const { response, error, status } = await apiQueryFetch(params);

  if (error) {
    if (status === 403) {
    }
  }
  if (response) {
    const data = normalizePlotData(response.data, plot, timeRange);
    // console.log(data);
    usePlotsDataStore.setState(
      produce<PlotsDataStore>((state) => {
        state.plotsData[plotKey] = mergeLeft(state.plotsData[plotKey], data);
      })
    );
  }
}

export function normalizePlotData(data: SeriesResponse, plot: PlotParams, timeRange: TimeRange): PlotData {
  const width = 2000;
  const result = getEmptyPlotData();

  // const promqltestfailed = !!data.promqltestfailed;
  const uniqueWhat: Set<QueryWhat> = new Set();
  const uniqueName = new Set();
  // const uniqueMetricType: Set<string> = new Set();
  let series_meta = [...data.series.series_meta];
  let series_data = [...data.series.series_data];
  const totalLineId = plot.totalLine ? series_meta.length : null;
  const totalLineLabel = 'Total';
  if (plot.totalLine) {
    const totalLineData = data.series.time.map((time, idx) => series_data.reduce((res, d) => res + (d[idx] ?? 0), 0));
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
    data.series.series_meta.forEach((series, indexSeries) => {
      const indexColor = colorIndex.get(series.color);
      if (series.color && indexColor != null) {
        data.series.series_data[indexSeries].forEach((value, indexValue) => {
          if (value != null) {
            series_data[indexColor][indexValue] = (series_data[indexColor][indexValue] ?? 0) + value;
          }
        });
      } else {
        const index = series_meta.push(series) - 1;
        series_data.push([...data.series.series_data[indexSeries]]);
        colorIndex.set(series.color, index);
      }
    });
  }

  for (const meta of series_meta) {
    if (isQueryWhat(meta.what)) {
      uniqueWhat.add(meta.what);
    }
    meta.name && uniqueName.add(meta.name);
    // if (meta.metric_type) {
    //   uniqueMetricType.add(meta.metric_type);
    // }
  }
  // const currentPrevState = getState();

  // const { lastPlotParams: currentPrevLastPlotParams, seriesShow: currentPrevSeriesShow } = getState().plotsData[index];
  // const currentPrevSeries = getState().plotsData[index].series.map((s) => ({ ...s, values: undefined }));
  // if (uniqueName.size === 0 && lastPlotParams.metricName !== promQLMetric) {
  //   uniqueName.add(lastPlotParams.metricName);
  // }
  // const metricType = uniqueMetricType.size === 1 ? [...uniqueMetricType.keys()][0] : '';

  // const maxLabelLength = Math.max(
  //   'Time'.length,
  //   ...series_meta.map((meta) => {
  //     const label = metaToLabel(meta, uniqueWhat.size);
  //     return label.length;
  //   })
  // );
  // const legendNameWidth = (series_meta.length ?? 0) > 5 ? maxLabelLength * pxPerChar : 1_000_000;
  // let legendMaxHostWidth = 0;
  // const legendMaxHostPercentWidth = 0;
  result.dataView = result.data = [data.series.time, ...series_data];

  // const stacked = lastPlotParams.type === PLOT_TYPE.Event ? stackData(data) : undefined;
  const usedDashes = {};
  const usedBaseColors = {};
  const baseColors: Record<string, string> = {};
  // let changeColor = false;
  // let changeType = currentPrevLastPlotParams?.type !== lastPlotParams.type;
  // const changeView =
  //   currentPrevLastPlotParams?.totalLine !== lastPlotParams.totalLine ||
  //   currentPrevLastPlotParams?.filledGraph !== lastPlotParams.filledGraph;
  const widthLine =
    (width ?? 0) > data.series.time.length ? (devicePixelRatio > 1 ? 2 / devicePixelRatio : 1) : 1 / devicePixelRatio;

  // const topInfoCounts: Record<string, number> = {};
  // const topInfoTotals: Record<string, number> = {};
  // let topInfo: TopInfo | undefined = undefined;
  // const maxHostLists: SelectOptionProps[][] = new Array(series_meta.length).fill([]);
  const oneGraph = series_meta.filter((s) => s.time_shift === 0).length <= 1;
  // const seriesShow: boolean[] = new Array(series_meta.length).fill(true);
  // const seriesTimeShift: number[] = [];
  result.series = series_meta.map((meta, indexMeta): uPlot.Series => {
    const timeShift = meta.time_shift !== 0;
    // seriesTimeShift[indexMeta] = meta.time_shift;
    const label = totalLineId !== indexMeta ? metaToLabel(meta, uniqueWhat.size) : totalLineLabel;
    const baseLabel = totalLineId !== indexMeta ? metaToBaseLabel(meta, uniqueWhat.size) : totalLineLabel;
    const isValue = baseLabel.indexOf('Value') === 0;
    const prefColor = '9'; // it`s magic prefix
    const metricName = isValue ? `${meta.name || (plot.metricName !== promQLMetric ? plot.metricName : '')}: ` : '';
    const colorKey = `${prefColor}${metricName}${oneGraph ? label : baseLabel}`;
    const baseColor = meta.color ?? baseColors[colorKey] ?? selectColor(colorKey, usedBaseColors);
    // baseColors[colorKey] = baseColor;
    // if (baseColor !== getState().plotsData[index]?.series[indexMeta]?.stroke) {
    //   changeColor = true;
    // }
    // if (meta.max_hosts) {
    //   const max_hosts_l = meta.max_hosts
    //     .map((host) => host.length * pxPerChar * 1.25 + 65)
    //     .filter(Boolean)
    //     .sort();
    //   const full = max_hosts_l[0] ?? 0;
    //   const p75 = max_hosts_l[Math.floor(max_hosts_l.length * 0.25)] ?? 0;
    //   legendMaxHostWidth = Math.max(legendMaxHostWidth, full - p75 > 20 ? p75 : full);
    // }
    // const max_host_map =
    //   meta.max_hosts?.reduce(
    //     (res, host) => {
    //       if (host) {
    //         res[host] = (res[host] ?? 0) + 1;
    //       }
    //       return res;
    //     },
    //     {} as Record<string, number>
    //   ) ?? {};
    // const max_host_total = meta.max_hosts?.filter(Boolean).length ?? 1;
    // seriesShow[indexMeta] = currentPrevSeries[indexMeta]?.label === label ? currentPrevSeriesShow[indexMeta] : true;
    // maxHostLists[indexMeta] = Object.entries(max_host_map)
    //   .sort(([k, a], [n, b]) => (a > b ? -1 : a < b ? 1 : k > n ? 1 : k < n ? -1 : 0))
    //   .map(([host, count]) => {
    //     const percent = formatPercent(count / max_host_total);
    //     return {
    //       value: host,
    //       title: `${host}: ${percent}`,
    //       name: `${host}: ${percent}`,
    //       html: `<div class="d-flex"><div class="flex-grow-1 me-2 overflow-hidden text-nowrap">${host}</div><div class="text-end">${percent}</div></div>`,
    //     };
    //   });
    // if (totalLineId !== indexMeta) {
    //   const key = `${meta.what}|${meta.time_shift}`;
    //   topInfoCounts[key] = (topInfoCounts[key] ?? 0) + 1;
    //   topInfoTotals[key] = meta.total;
    // }
    const paths =
      plot.type === PLOT_TYPE.Event
        ? uPlot.paths.bars!({ size: [0.7], gap: 0, align: 1 })
        : uPlot.paths.stepped!({
            align: 1,
          });
    return {
      show: true, //seriesShow[indexMeta] ?? true,
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
      // values(u, seriesIdx, idx): PlotValues {
      //   if (idx === null) {
      //     return {
      //       metricName: '',
      //       rawValue: null,
      //       value: '',
      //       label: '',
      //       baseLabel: '',
      //       timeShift: 0,
      //       max_host: '',
      //       total: 0,
      //       percent: '',
      //       max_host_percent: '',
      //       top_max_host: '',
      //       top_max_host_percent: '',
      //     };
      //   }
      //   const localData = (stacked ? getState().plotsData[index]?.data : u.data) ?? [];
      //   const rawValue = localData[seriesIdx]?.[idx] ?? null;
      //   let total = 0;
      //   for (let i = 1; i < u.series.length; i++) {
      //     const v = localData[i]?.[idx];
      //     if (v !== null && v !== undefined && i - 1 !== totalLineId) {
      //       total += v;
      //     }
      //   }
      //   const value = formatLegendValue(rawValue);
      //   const max_host = meta.max_hosts !== null && idx < meta.max_hosts.length ? meta.max_hosts[idx] : '';
      //
      //   const max_host_percent =
      //     meta.max_hosts !== null && max_host_map && meta.max_hosts[idx]
      //       ? formatPercent((max_host_map[meta.max_hosts[idx]] ?? 0) / max_host_total)
      //       : '';
      //   const percent = rawValue !== null ? formatPercent(rawValue / total) : '';
      //   return {
      //     metricName,
      //     rawValue,
      //     value,
      //     label,
      //     baseLabel,
      //     timeShift: meta.time_shift,
      //     max_host,
      //     total,
      //     percent: totalLineId !== indexMeta ? percent : '100%',
      //     max_host_percent,
      //     top_max_host: maxHostLists[indexMeta]?.[0]?.value ?? '',
      //     top_max_host_percent: maxHostLists[indexMeta]?.[0]?.title ?? '',
      //   };
      // },
    };
  });

  // const topInfoTop = {
  //   min: Math.min(...Object.values(topInfoCounts)),
  //   max: Math.max(...Object.values(topInfoCounts)),
  // };
  // const topInfoTotal = {
  //   min: Math.min(...Object.values(topInfoTotals)),
  //   max: Math.max(...Object.values(topInfoTotals)),
  // };
  // const topInfoFunc = lastPlotParams.what.length;
  // const topInfoShifts = currentPrevState.params.timeShifts.length;
  // const info: string[] = [];
  //
  // if (topInfoTop.min !== topInfoTotal.min && topInfoTop.max !== topInfoTotal.max) {
  //   if (topInfoFunc > 1) {
  //     info.push(`${topInfoFunc} functions`);
  //   }
  //   if (topInfoShifts > 0) {
  //     info.push(`${topInfoShifts} time-shift${topInfoShifts > 1 ? 's' : ''}`);
  //   }
  //   topInfo = {
  //     top: topInfoTop.max === topInfoTop.min ? topInfoTop.max.toString() : `${topInfoTop.min}-${topInfoTop.max}`,
  //     total:
  //       topInfoTotal.max === topInfoTotal.min ? topInfoTotal.max.toString() : `${topInfoTotal.min}-${topInfoTotal.max}`,
  //     info: info.length ? ` (${info.join(',')})` : '',
  //   };
  // }

  result.scales.x = { min: timeRange.from + timeRange.to, max: timeRange.to };
  if (plot.yLock.min !== 0 || plot.yLock.max !== 0) {
    result.scales.y = { ...plot.yLock };
  } else {
    result.scales.y = { min: 0, max: 0 };
  }

  // const maxLengthValue = series.reduce(
  //   (res, s, indexSeries) => {
  //     if (s.show) {
  //       const v =
  //         (data[indexSeries + 1] as (number | null)[] | undefined)?.reduce(
  //           (res2, d) => {
  //             if (d && (res2?.toString().length ?? 0) < d.toString().length) {
  //               return d;
  //             }
  //             return res2;
  //           },
  //           null as null | number
  //         ) ?? null;
  //       if (v && (v.toString().length ?? 0) > (res?.toString().length ?? 0)) {
  //         return v;
  //       }
  //     }
  //     return res;
  //   },
  //   null as null | number
  // );
  // const [yMinAll, yMaxAll] = calcYRange2(series, data, false);
  // const legendExampleValue = Math.max(Math.abs(Math.floor(yMinAll) - 0.001), Math.abs(Math.ceil(yMaxAll) + 0.001));
  // const legendValueWidth = (formatLegendValue(legendExampleValue).length + 2) * pxPerChar; // +2 - focus marker

  // const legendMaxDotSpaceWidth =
  //   Math.max(4, (formatLegendValue(maxLengthValue).split('.', 2)[1]?.length ?? 0) + 2) * pxPerChar;
  // const legendPercentWidth = (4 + 2) * pxPerChar; // +2 - focus marker
  // setState((state) => {
  //   delete state.plotsDataAbortController[index];
  //   const noUpdateData = dequal(stacked?.data || data, state.plotsData[index]?.stacked || state.plotsData[index]?.data);
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
  //     data: noUpdateData ? state.plotsData[index]?.data : data,
  //     stacked: noUpdateData ? state.plotsData[index]?.stacked : stacked?.data,
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

  return result;
}

export function plotsDataStoreSubscribe(state: PlotsInfoStore, prevState: PlotsInfoStore) {
  const urlStore = useUrlStore.getState();
  state.viewOrderPlot.forEach((plotKey) => {
    const plotInfo = state.plotsInfo[plotKey];
    const plot = urlStore.params.plots[plotKey];
    if (plot && plotInfo) {
      loadPlotData(plotKey, plot, urlStore.params.timeRange);
    }
  });
}

usePlotsInfoStore.subscribe(plotsDataStoreSubscribe);
