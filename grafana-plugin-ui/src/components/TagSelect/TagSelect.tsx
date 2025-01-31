// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useState } from 'react';
import { Key, Tag } from '../../types';
import { HorizontalGroup, InlineField, InlineSwitch, MultiSelect } from '@grafana/ui';
import { getDefaultTimeRange, SelectableValue, TimeRange } from '@grafana/data';
import { DataSource } from '../../datasource';
import { useMetricTagValues } from './useMetricTagValues';

export const TagSelect = memo(
  (props: {
    tag: Tag;
    datasource: DataSource;
    metricName: string;
    what: string[];
    selKey: Key | undefined;
    range: TimeRange | undefined;
    setKey: (tag: Tag, values: string[], groupBy: boolean, notIn: boolean) => void;
  }) => {
    const { tag, datasource, metricName, what, range, setKey } = props;
    const key = props.selKey ?? { values: [], groupBy: false, raw: tag.isRaw, notIn: false };

    const [customOptions, setCustomOptions] = useState<SelectableValue[]>([]);

    const { loading, error, setFocused, options } = useMetricTagValues(
      datasource,
      metricName,
      what,
      key,
      tag.id,
      range || getDefaultTimeRange()
    );

    let opts = options;
    if (customOptions.length > 0) {
      opts = [...customOptions, ...options];
    }

    return (
      <HorizontalGroup>
        <MultiSelect
          width={50}
          options={opts}
          isSearchable={true}
          isLoading={loading}
          disabled={!!error}
          allowCustomValue={true}
          onOpenMenu={() => setFocused(true)}
          onBlur={() => setFocused(false)}
          value={key.values}
          onCreateOption={(opt: string) => {
            setCustomOptions([...customOptions, ...[{ label: opt, value: opt }]]);
            setKey(tag, [...key.values, opt], key.groupBy, key.notIn);
          }}
          onChange={(opts) => {
            setKey(
              tag,
              opts.map((opt) => String(opt.value)),
              key.groupBy,
              key.notIn
            );
          }}
        />
        <InlineField label={'Not in'}>
          <InlineSwitch
            value={key.notIn}
            onChange={(e) => {
              setKey(tag, key.values, key.groupBy, e.currentTarget.checked);
            }}
          />
        </InlineField>
        <InlineField label={'Group by'}>
          <InlineSwitch
            value={key.groupBy}
            onChange={(e) => {
              setKey(tag, key.values, e.currentTarget.checked, key.notIn);
            }}
          />
        </InlineField>
      </HorizontalGroup>
    );
  }
);

TagSelect.displayName = 'TagSelect';
