import { produce } from 'immer';
import { createStore } from '../createStore';
import { apiMetricFetch, MetricMetaValue } from 'api/metric';
import { promiseRun } from 'common/promiseRun';
import { GET_PARAMS } from 'api/enum';
import { debug } from 'common/debug';
import { useErrorStore } from 'store';
import { promQLMetric } from '../urlStore';

export type MetricsStore = {
  meta: Partial<Record<string, MetricMetaValue>>;
};

export const useMetricsStore = createStore<MetricsStore>(() => ({
  meta: {},
}));

export async function loadMetricMeta(metricName: string) {
  if (!metricName || metricName === promQLMetric) {
    return null;
  }
  const meta = useMetricsStore.getState().meta[metricName];
  if (meta?.name) {
    return meta;
  }
  const requestKey = `loadMetricsMeta_${metricName}`;
  const [request, first] = promiseRun(requestKey, apiMetricFetch, { [GET_PARAMS.metricName]: metricName }, requestKey);
  // prevState.setGlobalNumQueriesPlot((n) => n + 1);
  const { response, error, status } = await request;
  // prevState.setGlobalNumQueriesPlot((n) => n - 1);
  if (first) {
    if (response) {
      debug.log('loading meta for', response.data.metric.name);
      useMetricsStore.setState(
        produce((state) => {
          state.meta[response.data.metric.name] = response.data.metric;
        })
      );
    }
    if (error) {
      if (status !== 403) {
        useErrorStore.getState().addError(error);
      }
    }
  }

  return useMetricsStore.getState().meta[metricName] ?? null;
}

export function clearMetricsMeta(metricName: string) {
  if (useMetricsStore.getState().meta[metricName]) {
    useMetricsStore.setState(
      produce((state) => {
        delete state.metricsMeta[metricName];
      })
    );
  }
}
