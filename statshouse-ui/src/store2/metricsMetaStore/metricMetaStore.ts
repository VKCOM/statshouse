// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { MetricMetaTag, MetricMetaValue } from '@/api/metric';
import { type TagKey, toTagKey } from '@/api/enum';

export type MetricMeta = MetricMetaValue & {
  tagsObject: Partial<Record<TagKey, MetricMetaTag>>;
  tagsOrder: TagKey[];
};

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
