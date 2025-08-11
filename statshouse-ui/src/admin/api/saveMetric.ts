// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { isNotNil } from '@/common/helpers';
import { IBackendKind, IKind, IMetric, ITag } from '../models/metric';
import { freeKeyPrefix } from '@/url2';
import { ApiMetric, MetricMetaValue } from '@/api/metric';

export function resetMetricFlood(metricName: string) {
  return fetch(`/api/reset-flood?s=${metricName}`, {
    method: 'POST',
    body: '',
  })
    .then((res) => res.json())
    .catch(() => {
      throw new Error('Unknown error');
    })
    .then((parsed) => {
      if ('error' in parsed) {
        throw new Error(parsed.error);
      }
    });
}

export function mapMetricToEdit({ data: { metric } }: ApiMetric): IMetric {
  const tags_draft: ITag[] = Object.entries(metric.tags_draft ?? {})
    .map(([, t]) => t as ITag)
    .filter(isNotNil);
  tags_draft.sort((a, b) => (b.name < a.name ? 1 : b.name === a.name ? 0 : -1));

  return {
    id: metric.metric_id === undefined ? 0 : metric.metric_id,
    name: metric.name,
    description: metric.description ?? '',
    kind: (metric.kind.endsWith('_p') ? metric.kind.replace('_p', '') : metric.kind) as IKind,
    stringTopName: metric.string_top_name === undefined ? '' : metric.string_top_name,
    stringTopDescription: metric.string_top_description === undefined ? '' : metric.string_top_description,
    weight: metric.weight === undefined ? 1 : metric.weight,
    resolution: metric.resolution === undefined ? 1 : metric.resolution,
    visible: metric.visible === undefined ? false : metric.visible,
    disable: metric.disable === undefined ? false : metric.disable,
    withPercentiles: metric.kind.endsWith('_p'),
    tags:
      metric.tags?.map((tag, index) => ({
        name: tag.name === undefined || tag.name === `key${index}` ? '' : tag.name,
        alias: tag.description === undefined ? '' : tag.description,
        customMapping: tag.value_comments
          ? Object.entries(tag.value_comments).map(([from, to]) => ({
              from,
              to,
            }))
          : [],
        isRaw: tag.raw || tag.raw_kind != null,
        raw_kind: tag.raw_kind,
      })) ?? [],
    tags_draft,
    tagsSize: metric.tags?.length ?? 0,
    pre_key_tag_id: metric.pre_key_tag_id && freeKeyPrefix(metric.pre_key_tag_id),
    pre_key_from: metric.pre_key_from,
    metric_type: metric.metric_type,
    version: metric.version,
    group_id: metric.group_id,
    fair_key_tag_ids: metric.fair_key_tag_ids,
    skip_max_host: !!metric.skip_max_host,
    skip_min_host: !!metric.skip_min_host,
    skip_sum_square: !!metric.skip_sum_square,
  };
}

export function mapEditToMetric(metric: IMetric): MetricMetaValue {
  return {
    description: metric.description,
    kind: (metric.kind + (metric.withPercentiles ? '_p' : '')) as IBackendKind,
    name: metric.name,
    metric_id: metric.id,
    string_top_name: metric.stringTopName,
    string_top_description: metric.stringTopDescription,
    weight: metric.weight,
    resolution: metric.resolution,
    visible: metric.visible,
    disable: metric.disable,
    tags: metric.tags.map((tag) => ({
      name: tag.name,
      description: tag.alias,
      raw: tag.raw_kind != null,
      raw_kind: tag.raw_kind,
      value_comments:
        tag.customMapping.length > 0
          ? tag.customMapping.reduce(
              (acc, map) => {
                acc[map.from] = map.to;
                return acc;
              },
              {} as Record<string, string>
            )
          : undefined,
    })),
    tags_draft: Object.fromEntries(metric.tags_draft.map((t) => [t.name, t])),
    pre_key_tag_id: metric.pre_key_tag_id,
    pre_key_from: metric.pre_key_from ? metric.pre_key_from : 0,
    skip_max_host: !!metric.skip_max_host,
    skip_min_host: !!metric.skip_min_host,
    skip_sum_square: !!metric.skip_sum_square,
    pre_key_only: !!metric.pre_key_only,
    metric_type: metric.metric_type,
    version: metric.version,
    group_id: metric.group_id,
    fair_key_tag_ids: metric.fair_key_tag_ids,
  };
}
