// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { StoreSlice } from '../createStore';
import { apiMetric, type MetricMetaTag, type MetricMetaValue } from '@/api/metric';
import { type TagKey, toTagKey } from '@/api/enum';
import type { StatsHouseStore } from '../statsHouseStore';
import { type PlotKey, promQLMetric } from '@/url2';
import { useErrorStore } from '@/store/errors';
import { debug } from '@/common/debug';
import { updateMetricMeta } from './updateMetricMeta';
import { ExtendedError } from '../../api/api';

export type MetricMeta = MetricMetaValue & {
  tagsObject: Partial<Record<TagKey, MetricMetaTag>>;
  tagsOrder: TagKey[];
};

export type MetricMetaStore = {
  metricMeta: Partial<Record<string, MetricMeta>>;
  loadMetricMeta(metricName: string): Promise<null | MetricMeta>;
  loadMetricMetaByPlotKey(plotKey: PlotKey): Promise<null | MetricMeta>;
  clearMetricMeta(metricName: string): void;
};

export const metricMetaStore: StoreSlice<StatsHouseStore, MetricMetaStore> = (setState, getState) => ({
  metricMeta: {},
  async loadMetricMeta(metricName) {
    if (!metricName || metricName === promQLMetric) {
      return null;
    }
    const meta = getState().metricMeta[metricName];
    if (meta?.name) {
      return meta;
    }

    const { response, error, status } = await apiMetric(metricName);

    if (response) {
      debug.log('loading meta for', response.data.metric.name);
      const metricMeta: MetricMeta = {
        ...response.data.metric,
        ...tagsArrToObject(response.data.metric.tags),
      };
      setState(updateMetricMeta(metricMeta.name, metricMeta));
    }
    if (error) {
      if (status !== 403 && error.status !== ExtendedError.ERROR_STATUS_ABORT) {
        useErrorStore.getState().addError(error);
      }
    }

    return getState().metricMeta[metricName] ?? null;
  },
  async loadMetricMetaByPlotKey(plotKey) {
    const {
      plotsData,
      params: { plots },
      loadMetricMeta,
    } = getState();
    const plotName = plotsData[plotKey]?.metricName ?? plots[plotKey]?.metricName ?? '';
    return loadMetricMeta(plotName);
  },
  clearMetricMeta(metricName) {
    setState(updateMetricMeta(metricName, null));
  },
});

export function tagsArrToObject(tags: MetricMetaTag[] = []): Pick<MetricMeta, 'tagsObject' | 'tagsOrder'> {
  const tagsObject: Partial<Record<TagKey, MetricMetaTag>> = {};
  const tagsOrder: TagKey[] = [];
  tags.forEach((tag, indexTag) => {
    const tagKey = toTagKey(indexTag);
    if (tagKey) {
      tagsObject[tagKey] = tag;
      tagsOrder.push(tagKey);
    }
  });
  return { tagsObject, tagsOrder };
}
