import { produce } from 'immer';
import { createStore } from '../createStore';
import { apiMetricFetch, MetricMetaTag, MetricMetaValue } from 'api/metric';
import { promiseRun } from 'common/promiseRun';
import { GET_PARAMS, TAG_KEY, TagKey, toTagKey } from 'api/enum';
import { debug } from 'common/debug';
import { useErrorStore } from 'store';
import { promQLMetric } from '../urlStore';

export type MetricsMeta = MetricMetaValue & {
  tagsObject: Partial<Record<TagKey, MetricMetaTag>>;
  tagsOrder: TagKey[];
};

export type MetricsStore = {
  meta: Partial<Record<string, MetricsMeta>>;
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
          const tagsObject = tagsArrToObject(response.data.metric.tags);
          state.meta[response.data.metric.name] = {
            ...response.data.metric,
            tagsObject,
            tagsOrder: Object.keys(tagsObject),
          };
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

export function tagsArrToObject(tags: MetricMetaTag[] = []): Partial<Record<TagKey, MetricMetaTag>> {
  return tags.reduce(
    (res, t, indexTag) => {
      res[toTagKey(indexTag, TAG_KEY._0)] = t;
      return res;
    },
    {} as Partial<Record<TagKey, MetricMetaTag>>
  );
}
