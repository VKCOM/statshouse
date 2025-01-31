// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useAsync } from 'react-use';
import type { DataSource } from '../../datasource';
import { DataQueryError, SelectableValue } from '@grafana/data';

type AsyncMetricNamesState = {
  loading: boolean;
  error: DataQueryError | undefined;
  metricNames: Array<SelectableValue<string>>;
};

export function useMetricNames(datasource: DataSource): AsyncMetricNamesState {
  const result = useAsync(async () => {
    const { metric_names } = await datasource.getAvailableMetricNames();

    return metric_names.map((metricName: string) => ({
      label: metricName,
      value: metricName,
    }));
  }, [datasource]);

  return {
    loading: result.loading,
    metricNames: result.value ?? [],
    error: result.error,
  };
}
