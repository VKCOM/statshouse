// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback, useMemo } from 'react';
import cn from 'classnames';
import { isMetricAggregation, METRIC_AGGREGATION, METRIC_AGGREGATION_DESCRIPTION } from '@/api/enum';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';

export type PlotControlAggregationProps = {
  className?: string;
};

const aggregationList = Object.values(METRIC_AGGREGATION).map((value) => ({
  value,
  description: METRIC_AGGREGATION_DESCRIPTION[value],
}));

export const PlotControlAggregation = memo(function PlotControlAggregation({ className }: PlotControlAggregationProps) {
  const {
    plot: { customAgg },
    setPlot,
  } = useWidgetPlotContext();

  const onChangeAgg = useCallback(
    (e: React.ChangeEvent<HTMLSelectElement>) => {
      const customAgg = parseInt(e.currentTarget.value);
      setPlot((p) => {
        p.customAgg = customAgg;
      });
    },
    [setPlot]
  );
  const otherAgg = useMemo(() => !isMetricAggregation(customAgg), [customAgg]);
  return (
    <select
      className={cn('form-select', customAgg > 0 && 'border-warning', otherAgg && 'border-danger', className)}
      value={customAgg}
      onChange={onChangeAgg}
    >
      {otherAgg && (
        <option value={customAgg} disabled>
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
});
