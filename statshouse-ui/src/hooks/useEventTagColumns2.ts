// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useMemo } from 'react';
import { ApiTableRowNormalize } from '../api/tableOld';
import { PlotParams } from 'url2';
import { MetricMetaValue } from '../api/metric';
import { toTagKey } from '../url/queryParams';
import { TAG_KEY } from '../api/enum';
import { useStatsHouse } from 'store2';
import { getTagDescription } from '../view/utils2';

export function getEventTagColumns(plot?: PlotParams, meta?: MetricMetaValue, selectedOnly: boolean = false) {
  if (plot == null) {
    return [];
  }
  const columns: UseEventTagColumnReturn2[] = (meta?.tags ?? [])
    .map((tag, indexTag) => {
      const tagKey = toTagKey(indexTag.toString());
      if (tagKey) {
        const disabled = plot.groupBy.indexOf(tagKey) > -1;
        const selected = disabled || plot.eventsBy.indexOf(tagKey) > -1;
        const hide = !selected || plot.eventsHide.indexOf(tagKey) > -1;
        if ((!selectedOnly || (selected && !hide)) && tag.description !== '-') {
          return {
            keyTag: tagKey,
            name: getTagDescription(meta, indexTag),
            selected,
            disabled,
            hide,
          };
        }
      }
      return null;
    })
    .filter(Boolean) as UseEventTagColumnReturn2[];
  const disabled_s = plot.groupBy.indexOf(TAG_KEY._s) > -1;
  const selected_s = disabled_s || plot.eventsBy.indexOf(TAG_KEY._s) > -1;
  const hide_s = !selected_s || plot.eventsHide.indexOf(TAG_KEY._s) > -1;
  if ((!selectedOnly || (selected_s && !hide_s)) && (meta?.string_top_name || meta?.string_top_description)) {
    columns.push({
      keyTag: TAG_KEY._s,
      fullKeyTag: 'skey',
      name: getTagDescription(meta, TAG_KEY._s),
      selected: selected_s,
      disabled: disabled_s,
      hide: hide_s,
    });
  }
  return columns;
}

export type UseEventTagColumnReturn2 = {
  keyTag: keyof ApiTableRowNormalize;
  fullKeyTag: string;
  name: string;
  selected: boolean;
  disabled: boolean;
  hide: boolean;
};

export function useEventTagColumns2(plot?: PlotParams, selectedOnly: boolean = false): UseEventTagColumnReturn2[] {
  const meta = useStatsHouse((s) => s.metricMeta[plot?.metricName ?? '']);
  return useMemo(() => getEventTagColumns(plot, meta, selectedOnly), [meta, plot, selectedOnly]);
}
