// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useWidgetParamsContext, useWidgetPlotContext } from '@/contexts';
import { ApiQuery, useApiQuery } from '@/api/query';
import { useMemo } from 'react';
import { isPromQL } from '@/store2/helpers';
import { uniqueArray } from '@/common/helpers';

const metricSelector = (res?: ApiQuery) => res?.data?.series.series_meta;

export function useMetricWhats() {
  const { plot } = useWidgetPlotContext();
  const { what } = plot;
  const isProm = isPromQL(plot);
  const { params } = useWidgetParamsContext();
  const queryData = useApiQuery(plot, params, metricSelector);
  const series_meta = queryData.data;

  return useMemo(() => {
    if (!isProm) {
      return [...what];
    }
    if (series_meta) {
      return uniqueArray(series_meta.map((s) => s.what));
    }
    return [];
  }, [isProm, series_meta, what]);
}
