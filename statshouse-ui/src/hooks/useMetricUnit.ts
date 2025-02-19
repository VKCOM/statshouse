// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useWidgetParamsContext, useWidgetPlotContext } from '@/contexts';
import { ApiQuery, useApiQuery } from '@/api/query';
import { useMemo } from 'react';
import { METRIC_TYPE, toMetricType } from '@/api/enum';
import { getMetricType } from '@/common/formatByMetricType';
import { uniqueArray } from '@/common/helpers';
import { useMetricWhats } from './useMetricWhats';
import { useMetricMeta } from '@/hooks/useMetricMeta';
import { useMetricName } from '@/hooks/useMetricName';

const metricSelector = (res?: ApiQuery) => res?.data?.series.series_meta;

export function useMetricUnit() {
  const { plot } = useWidgetPlotContext();
  const { metricUnit } = plot;
  const { params } = useWidgetParamsContext();
  const queryData = useApiQuery(plot, params, metricSelector);
  const series_meta = queryData.data;
  const whats = useMetricWhats();
  const meta = useMetricMeta(useMetricName(true));

  return useMemo(() => {
    if (metricUnit != null) {
      return metricUnit;
    }
    if (series_meta) {
      const dataUnit = uniqueArray(series_meta.map((s) => toMetricType(s.metric_type, METRIC_TYPE.none)));
      return getMetricType(whats, dataUnit.length === 1 ? dataUnit[0] : METRIC_TYPE.none);
    }
    if (meta?.metric_type) {
      return meta.metric_type;
    }
    return METRIC_TYPE.none;
  }, [meta?.metric_type, metricUnit, series_meta, whats]);
}
