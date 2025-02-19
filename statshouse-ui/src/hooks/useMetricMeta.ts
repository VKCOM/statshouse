import { useMemo } from 'react';
import { MetricMeta, tagsArrToObject } from '@/store2/metricsMetaStore';
import { useApiMetric } from '@/api/metric';

export function useMetricMeta(metricName: string) {
  const metaData = useApiMetric(metricName);
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
