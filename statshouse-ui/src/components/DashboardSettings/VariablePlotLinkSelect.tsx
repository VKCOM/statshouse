// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback } from 'react';
import { MetricMetaValue } from '../../api/metric';
import { toTagKey } from '../../url/queryParams';
import { TAG_KEY, TagKey } from '../../api/enum';
import { getTagDescription, isTagEnabled } from '../../view/utils2';

export type VariablePlotLinkSelectProps = {
  indexPlot: number;
  selectTag?: TagKey;
  metricMeta?: MetricMetaValue;
  onChange?: (indexPlot: number, selectTag?: TagKey) => void;
};
export function VariablePlotLinkSelect({ indexPlot, selectTag, metricMeta, onChange }: VariablePlotLinkSelectProps) {
  const changeTag = useCallback(
    (e: React.ChangeEvent<HTMLSelectElement>) => {
      const iTag = toTagKey(e.currentTarget.value) ?? undefined;
      onChange?.(indexPlot, iTag);
    },
    [indexPlot, onChange]
  );
  return (
    <select className="form-select form-select-sm" value={selectTag?.toString() ?? 'null'} onChange={changeTag}>
      <option value="null">-</option>
      {metricMeta?.tags?.map((tag, indexTag) => {
        const keyTag = toTagKey(indexTag);
        return (
          keyTag != null &&
          isTagEnabled(metricMeta, keyTag) && (
            <option key={indexTag} value={keyTag}>
              {getTagDescription(metricMeta, keyTag)}
            </option>
          )
        );
      })}
      {isTagEnabled(metricMeta, TAG_KEY._s) && (
        <option value={TAG_KEY._s}>{getTagDescription(metricMeta, TAG_KEY._s)}</option>
      )}
    </select>
  );
}
