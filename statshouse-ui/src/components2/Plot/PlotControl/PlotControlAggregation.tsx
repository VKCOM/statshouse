// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback, useMemo } from 'react';
import cn from 'classnames';
import { isMetricAggregation, METRIC_AGGREGATION, METRIC_AGGREGATION_DESCRIPTION } from 'api/enum';
import { getNewPlot, type PlotKey, setPlot, useUrlStore } from '../../../store2';

export type PlotControlAggregationProps = {
  plotKey: PlotKey;
  className?: string;
};

const aggregationList = Object.values(METRIC_AGGREGATION).map((value) => ({
  value,
  description: METRIC_AGGREGATION_DESCRIPTION[value],
}));

const defaultCustomAgg = getNewPlot().customAgg;

export function _PlotControlAggregation({ className, plotKey }: PlotControlAggregationProps) {
  const value = useUrlStore((s) => s.params.plots[plotKey]?.customAgg ?? defaultCustomAgg);
  const onChangeAgg = useCallback(
    (e: React.ChangeEvent<HTMLSelectElement>) => {
      const customAgg = parseInt(e.currentTarget.value);
      setPlot(plotKey, (p) => {
        p.customAgg = customAgg;
      });
    },
    [plotKey]
  );
  const otherAgg = useMemo(() => !isMetricAggregation(value), [value]);
  return (
    <select
      className={cn('form-select', value > 0 && 'border-warning', otherAgg && 'border-danger', className)}
      value={value}
      onChange={onChangeAgg}
    >
      {otherAgg && (
        <option value={value} disabled>
          Other
        </option>
      )}
      {aggregationList.map(({ value, description }) => (
        <option key={value} value={value}>
          {description}
        </option>
      ))}
    </select>
  );
}
export const PlotControlAggregation = memo(_PlotControlAggregation);
