// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { ApiQuery, useApiQuery } from '@/api/query';
import { useWidgetParamsContext, useWidgetPlotContext } from '@/contexts';
import { useMemo } from 'react';
import { emptyArray, uniqueArray } from '@/common/helpers';
import { isPromQL } from '@/store2/helpers';

const metricNameByQuerySelector = (res?: ApiQuery) => {
  const seriesNames = uniqueArray(res?.data?.series.series_meta.map((s) => s.name) ?? emptyArray);
  return seriesNames.length === 1 ? seriesNames[0] : '';
};

export function useMetricName(real: boolean = false) {
  const { plot } = useWidgetPlotContext();
  const { id, metricName } = plot;
  const isProm = isPromQL(plot);
  const { params } = useWidgetParamsContext();
  const queryData = useApiQuery(plot, params, metricNameByQuerySelector, isProm);
  const seriesMetricName = queryData.data;
  return useMemo(() => {
    const nameById = real ? '' : `plot#${id}`;
    if (!isProm) {
      return metricName || nameById;
    }
    return seriesMetricName || nameById;
  }, [id, isProm, metricName, real, seriesMetricName]);
}
