// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { IBackendKind, IBackendMetric, IMetric } from '../models/metric';

export function saveMetric(metric: IMetric) {
  const body: IBackendMetric = {
    description: metric.description,
    kind: (metric.kind + (metric.withPercentiles ? '_p' : '')) as IBackendKind,
    name: metric.name,
    metric_id: metric.id,
    string_top_name: metric.stringTopName,
    string_top_description: metric.stringTopDescription,
    weight: metric.weight,
    resolution: metric.resolution,
    visible: metric.visible,
    tags: metric.tags.map((tag) => ({
      name: tag.name,
      description: tag.alias,
      raw: tag.isRaw || false,
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

  return fetch(`/api/metric${metric.id ? `?s=${metric.name}` : ''}`, {
    method: 'POST',
    body: JSON.stringify({ metric: body }),
  })
    .then((res) => res.json())
    .catch(() => {
      throw new Error('Unknown error');
    })
    .then((parsed) => {
      if ('error' in parsed) {
        throw new Error(parsed.error);
      }
      return parsed;
    });
}

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
