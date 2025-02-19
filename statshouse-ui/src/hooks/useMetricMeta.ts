// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useMemo } from 'react';
import { MetricMeta, tagsArrToObject } from '@/store2/metricsMetaStore';
import { useApiMetric } from '@/api/metric';

export function useMetricMeta(metricName: string, enabled: boolean = false) {
  const metaData = useApiMetric(metricName, undefined, enabled);
  const meta = metaData.data?.data.metric;
  return useMemo<MetricMeta | undefined>(() => {
    if (meta) {
      return {
        ...meta,
        ...tagsArrToObject(meta.tags),
      };
    }
    return undefined;
  }, [meta]);
}
