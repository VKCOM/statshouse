// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useAsync } from 'react-use';
import type { DataSource } from '../../datasource';
import { DataQueryError, SelectableValue } from '@grafana/data';
import { Tag } from '../../types';
import { useMemo } from 'react';

type AsyncMetricPropertiesState = {
  loading: boolean;
  error: DataQueryError | undefined;
  functions: Array<SelectableValue<string>>;
  tags: Tag[];
};

const labelMap: { [value: string]: string } = {
  p999: 'p99.9',
  count_norm: 'count/sec',
  cu_count: 'count (cumul)',
  cu_avg: 'avg (cumul)',
  sum_norm: 'sum/sec',
  cu_sum: 'sum (cumul)',
  unique_norm: 'unique/sec',
  max_count_host: 'max(count)@host',
  max_host: 'max(value)@host',
  dv_count: 'count (derivative)',
  dv_sum: 'sum (derivative)',
  dv_avg: 'avg (derivative)',
  dv_count_norm: 'count/sec (derivative)',
  dv_sum_norm: 'sum/sec (derivative)',
  dv_min: 'min (derivative)',
  dv_max: 'max (derivative)',
  dv_unique: 'unique (derivative)',
  dv_unique_norm: 'unique/sec (derivative)',
};

export function useMetricProperties(datasource: DataSource, metricName: string): AsyncMetricPropertiesState {
  const result = useAsync(async () => {
    return await datasource.getMetricProperties(metricName);
  }, [metricName]);

  const functions = useMemo(() => {
    return (
      result.value?.functions.map((func: string) => ({
        label: labelMap[func] ?? func,
        value: func,
      })) ?? []
    );
  }, [result.value]);

  if (result.value) {
    const { tags } = result.value;
    return {
      loading: result.loading,
      functions: functions,
      error: result.error,
      tags: tags,
    };
  }

  return {
    loading: result.loading,
    functions: [],
    error: result.error,
    tags: [],
  };
}
