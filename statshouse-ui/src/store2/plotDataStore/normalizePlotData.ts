// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { SeriesResponse } from '@/api/query';
import { type PlotParams, promQLMetric, type TimeRange } from '@/url2';
import type { ProduceUpdate } from '../helpers';
import { isQueryWhat, METRIC_TYPE, PLOT_TYPE, QUERY_WHAT, type QueryWhat, toMetricType } from '@/api/enum';
import uPlot from 'uplot';
import { PlotData, PlotDataSeries, PlotValues } from './plotsDataStore';
import { metaToBaseLabel, metaToLabel } from '@/view/api';
import { pxPerChar } from '@/common/settings';
import { stackData } from '@/common/stackData';
import { rgba, selectColor } from '@/view/palette';
import { filterPoints } from '@/common/filterPoints';
import { dequal } from 'dequal/lite';
import { calcYRange2 } from '@/common/calcYRange';
import { getEmptyPlotData } from './getEmptyPlotData';
import { deepClone } from '@/common/helpers';
import { formatLegendValue, timeShiftToDash } from '@/view/utils2';
import { useThemeStore } from '../themeStore';
import { PlotType } from '@/url/queryParams';

export function normalizePlotData(
  response: SeriesResponse,
  plot: PlotParams,
  timeRange: TimeRange,
  timeShifts: number[]
): ProduceUpdate<PlotData> {
  const width = 2000;
  return (plotData = getEmptyPlotData()) => {
    const { lastPlotParams: currentPrevLastPlotParams } = plotData;
    plotData.promqltestfailed = response.promqltestfailed;

    const uniqueWhat: Set<QueryWhat> = new Set();
    const uniqueName: Set<string> = new Set();
    const uniqueMetricType: Set<string> = new Set();
    let series_meta = [...response.series.series_meta];
    let series_data = [...response.series.series_data];
    const totalLineId = plot.totalLine ? series_meta.length : null;
    const totalLineLabel = 'Total';
    const totalLineColor = useThemeStore.getState().dark ? '#999999' : '#333333';
    const prefColor = '9'; // it`s magic prefix
    const usedDashes = {};
    const usedBaseColors = {};
    const baseColors: Record<string, string> = {};

    if (plot.type === PLOT_TYPE.Event && response.series.series_meta.length > 0) {
      series_meta = [];
      series_data = [];
      const colorIndex = new Map<string, number>();
      response.series.series_meta.forEach((series, indexSeries) => {
        const indexColor = colorIndex.get(series.color + '_' + series.time_shift);
        if (series.color && indexColor != null) {
          response.series.series_data[indexSeries].forEach((value, indexValue) => {
            if (value != null) {
              series_data[indexColor][indexValue] = (series_data[indexColor][indexValue] ?? 0) + value;
            }
          });
        } else {
          const index = series_meta.push(series) - 1;
          series_data.push([...response.series.series_data[indexSeries]]);
          colorIndex.set(series.color + '_' + series.time_shift, index);
        }
      });
    }

    if (response.metric?.name) {
      uniqueName.add(response.metric.name);
    }
    if (response.metric?.metric_type != null) {
      uniqueMetricType.add(response.metric.metric_type);
    }
    for (const meta of series_meta) {
      if (isQueryWhat(meta.what)) {
        uniqueWhat.add(meta.what);
      }
      if (meta.name) {
        uniqueName.add(meta.name);
      }
      if (meta.metric_type && meta.name !== plot.metricName) {
        uniqueMetricType.add(meta.metric_type);
      }
    }

    if (plot.totalLine) {
      const totalLineData = response.series.time.map((_time, idx) =>
        series_data.reduce((res, d) => res + (d[idx] ?? 0), 0)
      );
      series_meta.push({
        name: totalLineLabel,
        time_shift: 0,
        tags: { '0': { value: totalLineLabel } },
        max_hosts: null,
        what: QUERY_WHAT.sum,
        total: 0,
        color: totalLineColor,
      });
      baseColors[`${prefColor}${totalLineLabel}`] = totalLineColor;
      series_data.push(totalLineData);
    }

    if (uniqueName.size === 0 && currentPrevLastPlotParams && currentPrevLastPlotParams.metricName !== promQLMetric) {
      uniqueName.add(currentPrevLastPlotParams.metricName);
    }
    plotData.metricName = uniqueName.size === 1 ? [...uniqueName.keys()][0] : '';
    const whats = uniqueName.size === 1 ? [...uniqueWhat.keys()] : [];

    if (!dequal(plotData.whats, whats)) {
      plotData.whats = whats;
    }
    plotData.metricUnit =
      uniqueMetricType.size === 1 ? toMetricType([...uniqueMetricType.keys()][0], METRIC_TYPE.none) : METRIC_TYPE.none;

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
    plotData.bands = undefined;

    if (plot?.type === PLOT_TYPE.Event) {
      const stacked = stackData(plotData.data);
      plotData.dataView = stacked.data;
      plotData.bands = stacked.bands;
    }

    const widthLine =
      (width ?? 0) > response.series.time.length
        ? devicePixelRatio > 1
          ? 2 / devicePixelRatio
          : 1
        : 1 / devicePixelRatio;

    const topInfoCounts: Record<string, number> = {};
    const topInfoTotals: Record<string, number> = {};
    plotData.topInfo = undefined;
    const oneGraph = series_meta.filter((s) => s.time_shift === 0).length <= 1;
    const seriesShow: boolean[] = new Array(series_meta.length).fill(true);
    const seriesTimeShift: number[] = [];
    const series = series_meta.map((meta, indexMeta): PlotDataSeries => {
      const timeShift = meta.time_shift !== 0;
      // TimeShift = 1 for total line
      seriesTimeShift[indexMeta] = totalLineId !== indexMeta ? meta.time_shift : 1;
      const label = totalLineId !== indexMeta ? metaToLabel(meta, uniqueWhat.size) : totalLineLabel;
      const baseLabel = totalLineId !== indexMeta ? metaToBaseLabel(meta, uniqueWhat.size) : totalLineLabel;
      const isValue = baseLabel.indexOf('Value') === 0;
      if (plotData.series[indexMeta]?.label === label) {
        seriesShow[indexMeta] = plotData.seriesShow[indexMeta];
      }
      const metricName = isValue ? `${meta.name || (plot.metricName !== promQLMetric ? plot.metricName : '')}: ` : '';
      const colorKey = `${prefColor}${metricName}${oneGraph ? label : baseLabel}`;
      // client select color line
      const baseColor = meta.color ?? baseColors[colorKey] ?? selectColor(colorKey, usedBaseColors);
      baseColors[colorKey] = baseColor;
      if (meta.max_hosts) {
        const max_hosts_l = meta.max_hosts
          .map((host) => host.length)
          .filter(Boolean)
          .sort((a, b) => b - a);
        const full = (max_hosts_l[0] ?? 0) * pxPerChar * 1.25 + 65;
        const p75 = (max_hosts_l[Math.floor(max_hosts_l.length * 0.25)] ?? 0) * pxPerChar * 1.25 + 65;
        plotData.legendMaxHostWidth = Math.max(plotData.legendMaxHostWidth, full - p75 > 20 ? p75 : full);
      }
      if (totalLineId !== indexMeta) {
        const key = `${meta.what}|${meta.time_shift}`;
        topInfoCounts[key] = (topInfoCounts[key] ?? 0) + 1;
        topInfoTotals[key] = meta.total;
      }
      return {
        show: seriesShow[indexMeta] ?? true,
        auto: false, // we control the scaling manually
        label: label,
        stroke: baseColor,
        width: widthLine,
        dash: timeShift ? timeShiftToDash(meta.time_shift, usedDashes) : undefined,
        fill: totalLineId !== indexMeta && plot.filledGraph ? rgba(baseColor, timeShift ? 0.1 : 0.15) : undefined,
        points: PointsType[plot.type],
        paths: PathsType[plot.type],
        values: getLegendValues,
      };
    });
    if (!dequal(plotData.series, series)) {
      plotData.series = series;
    }
    if (!dequal(plotData.seriesTimeShift, seriesTimeShift)) {
      plotData.seriesTimeShift = seriesTimeShift;
    }
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

    plotData.promQL = response.promql;
    plotData.lastPlotParams = deepClone(plot);
    plotData.lastTimeRange = deepClone(timeRange);
    plotData.lastTimeShifts = deepClone(timeShifts);

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

    plotData.receiveErrors = response.receive_errors;
    plotData.receiveWarnings = response.receive_warnings;
    plotData.samplingFactorSrc = response.sampling_factor_src;
    plotData.samplingFactorAgg = response.sampling_factor_agg;
    plotData.mappingFloodEvents = response.mapping_errors;
  };
}

export function getLegendValues(u: uPlot, seriesIdx: number, idx: number | null): PlotValues {
  if (idx === null) {
    return {
      rawValue: null,
      value: '',
      seriesIdx: 0,
      idx: null,
    };
  }
  const rawValue = u.data[seriesIdx]?.[idx] ?? null;
  const value = formatLegendValue(rawValue);
  return {
    rawValue,
    value,
    seriesIdx,
    idx,
  };
}

export const PointsType: Record<PlotType, uPlot.Series.Points> = {
  [PLOT_TYPE.Metric]: {
    filter: filterPoints,
    size: 5,
  },
  [PLOT_TYPE.Event]: { show: false, size: 0 },
};

export const PathsType: Record<PlotType, uPlot.Series.PathBuilder> = {
  [PLOT_TYPE.Metric]: uPlot.paths.stepped!({
    align: 1,
  }),
  [PLOT_TYPE.Event]: uPlot.paths.bars!({ size: [0.7], gap: 0, align: 1 }),
};
