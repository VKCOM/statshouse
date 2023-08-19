// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback } from 'react';
import { getTagDescription, isTagEnabled } from '../../view/utils';
import { MetricMetaValue } from '../../api/metric';
import { toKeyTag } from '../../url/queryParams';
import { TAG_KEY } from '../../api/enum';

export type VariablePlotLinkSelectProps = {
  indexPlot: number;
  selectTag?: number;
  metricMeta?: MetricMetaValue;
  onChange?: (indexPlot: number, selectTag?: number) => void;
};
export function VariablePlotLinkSelect({ indexPlot, selectTag, metricMeta, onChange }: VariablePlotLinkSelectProps) {
  const changeTag = useCallback(
    (e: React.ChangeEvent<HTMLSelectElement>) => {
      const iTag = JSON.parse(e.currentTarget.value) ?? undefined;
      onChange?.(indexPlot, iTag);
    },
    [indexPlot, onChange]
  );
  return (
    <select className="form-select form-select-sm" value={selectTag?.toString() ?? 'null'} onChange={changeTag}>
      <option value="null">-</option>
      {metricMeta?.tags?.map(
        (tag, indexTag) =>
          isTagEnabled(metricMeta, toKeyTag(indexTag)) && (
            <option key={indexTag} value={indexTag}>
              {getTagDescription(metricMeta, indexTag)}
            </option>
          )
      )}
      {isTagEnabled(metricMeta, TAG_KEY._s) && <option value="-1">{getTagDescription(metricMeta, -1)}</option>}
    </select>
  );
}
