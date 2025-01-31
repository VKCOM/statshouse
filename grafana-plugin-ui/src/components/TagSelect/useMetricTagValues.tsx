// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useAsync } from 'react-use';
import type { DataSource } from '../../datasource';
import { SelectableValue, TimeRange } from '@grafana/data';
import { Key } from '../../types';
import React, { useState } from 'react';

type AsyncMetricTagValuesState = {
  loading: boolean;
  error: Error | undefined;
  setFocused: React.Dispatch<React.SetStateAction<boolean>>;
  options: Array<SelectableValue<string>>;
};

const getLabel = (value: string) => {
  if (value === ' 0') {
    return '⚡ unspecified';
  }

  if (value.startsWith(' ')) {
    return '⚡' + value;
  }

  return value;
};

export function useMetricTagValues(
  datasource: DataSource,
  metricName: string,
  what: string[],
  key: Key,
  tagId: string,
  timeRange: TimeRange
): AsyncMetricTagValuesState {
  const [focused, setFocused] = useState(false);
  const [options, setOptions] = useState<Array<SelectableValue<string>>>(
    key.values.map((v) => {
      return {
        label: getLabel(v),
        value: v,
      };
    }) ?? []
  );

  const result = useAsync(async () => {
    if (!focused) {
      return;
    }

    const { tag_values } = await datasource.getMetricTagValues(
      metricName,
      what,
      tagId,
      [],
      Math.floor(timeRange.from.toDate().getTime() / 1000),
      Math.floor(timeRange.to.toDate().getTime() / 1000)
    );
    setOptions(
      tag_values.map((tagValue) => ({
        label: getLabel(tagValue.value),
        value: tagValue.value,
      }))
    );
  }, [focused, metricName, what]);

  return {
    loading: result.loading,
    error: result.error,
    setFocused,
    options,
  };
}
