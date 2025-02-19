// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { useCallback, useMemo } from 'react';
import { METRIC_TYPE } from '@/api/enum';
import { getMetricType } from '@/common/formatByMetricType';
import { useMetricWhats } from './useMetricWhats';
import { useMetricMeta } from '@/hooks/useMetricMeta';
import { useMetricName } from '@/hooks/useMetricName';
import { usePlotsDataStore } from '@/store2/plotDataStore';

export function useMetricUnit() {
  const { plot } = useWidgetPlotContext();
  const { metricUnit } = plot;
  const dataMetricUnit = usePlotsDataStore(useCallback(({ plotsData }) => plotsData[plot.id]?.metricUnit, [plot.id]));
  const whats = useMetricWhats();
  const meta = useMetricMeta(useMetricName(true));

  return useMemo(() => {
    if (metricUnit != null) {
      return metricUnit;
    }
    if (dataMetricUnit) {
      return getMetricType(whats, dataMetricUnit);
    }
    if (meta?.metric_type) {
      return meta.metric_type;
    }
    return METRIC_TYPE.none;
  }, [dataMetricUnit, meta?.metric_type, metricUnit, whats]);
}
