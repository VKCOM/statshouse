// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useMemo } from 'react';
import uPlot from 'uplot';
import { isQueryWhat, type QueryWhat } from '@/api/enum';
import { paths, prefColor } from '@/components2/PlotWidgets/MetricWidget/constant';
import { PlotParams, promQLMetric } from '@/url2';
import { metaToBaseLabel, metaToLabel } from '@/view/api';
import { selectColorGenerator } from '@/view/palette';
import { timeShiftToDashGenerator } from '@/view/utils2';
import { filterPoints } from '@/common/filterPoints';
import { useWidgetParamsContext, useWidgetPlotContext } from '@/contexts';
import { ApiQuery, type QuerySeries, useApiQuery } from '@/api/query';
import { autoAgg } from '@/store2';
import { emptyArray } from '@/common/helpers';
import { seriesValues } from '@/common/seriesValues';

const metricSelector = (res?: ApiQuery) => res?.data?.series;

export function useMetricSeries(): uPlot.Series[] {
  const { plot } = useWidgetPlotContext();
  const { params } = useWidgetParamsContext();
  const queryData = useApiQuery(plot, params, metricSelector);
  const series = queryData.data;
  return useMemo(() => {
    if (!series || !plot) {
      return emptyArray;
    }
    return getMetricSeries(series, plot);
  }, [plot, series]);
}

export function getMetricSeries(series: QuerySeries, plot: PlotParams): uPlot.Series[] {
  const selectColor = selectColorGenerator();
  const timeShiftToDash = timeShiftToDashGenerator();

  const uniqueWhat: Set<QueryWhat> = new Set();

  for (const meta of series.series_meta) {
    if (isQueryWhat(meta.what)) {
      uniqueWhat.add(meta.what);
    }
  }

  const oneGraph = series.series_meta.filter((s) => s.time_shift === 0).length <= 1;
  return series.series_meta.map((meta): uPlot.Series => {
    const time_shift = meta.time_shift !== 0;
    const label = metaToLabel(meta, uniqueWhat.size);
    const baseLabel = metaToBaseLabel(meta, uniqueWhat.size);
    const isValue = baseLabel.indexOf('Value') === 0;

    const metricName = isValue ? `${meta.name || (plot.metricName !== promQLMetric ? plot.metricName : '')}: ` : '';
    const colorKey = `${prefColor}${metricName}${oneGraph ? label : baseLabel}`;

    const { stroke, fill } = selectColor(
      colorKey,
      meta.color,
      plot.filledGraph ? (time_shift ? 0.1 : 0.15) : undefined
    );

    const dash = timeShiftToDash(meta.time_shift);
    const show = true;
    return {
      show,
      auto: false, // we control the scaling manually
      label,
      stroke,
      width: autoAgg > series.time.length ? (devicePixelRatio > 1 ? 2 / devicePixelRatio : 1) : 1 / devicePixelRatio,
      dash,
      fill,
      points: {
        filter: filterPoints,
        size: 5,
      },
      paths,
      values: seriesValues,
    };
  });
}
