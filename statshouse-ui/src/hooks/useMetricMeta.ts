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
