// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo } from 'react';
import { DataSource } from '../../datasource';
import { Select } from '@grafana/ui';
import { SelectableValue } from '@grafana/data';
import { useMetricNames } from './useMetricNames';

export const MetricSelect = memo(
  (props: { datasource: DataSource; selectedMetricName: string; setMetric: (metricName: string) => void }) => {
    const { datasource, selectedMetricName, setMetric } = props;
    const { loading, metricNames, error } = useMetricNames(datasource);
    return (
      <Select
        width={50}
        options={
          metricNames.length > 0
            ? metricNames
            : [
                {
                  label: selectedMetricName,
                  value: selectedMetricName,
                },
              ]
        }
        onChange={(option) => {
          setMetric(String(option.value));
        }}
        isLoading={loading}
        disabled={!!error}
        value={selectedMetricName}
        filterOption={(option: SelectableValue, searchQuery: string) => {
          return option.value.includes(searchQuery);
        }}
      />
    );
  }
);

MetricSelect.displayName = 'MetricSelect';
