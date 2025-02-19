// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback } from 'react';
import { METRIC_TYPE, METRIC_TYPE_DESCRIPTION, type MetricType, toMetricType } from '@/api/enum';
import cn from 'classnames';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { useMetricUnit } from '@/hooks/useMetricUnit';

const METRIC_TYPE_KEYS: MetricType[] = ['null', ...Object.values(METRIC_TYPE)] as MetricType[];
const METRIC_TYPE_DESCRIPTION_SELECTOR = {
  null: 'infer unit',
  ...METRIC_TYPE_DESCRIPTION,
};

export type PlotControlUnitProps = {
  className?: string;
};

export const PlotControlUnit = memo(function PlotControlUnit({ className }: PlotControlUnitProps) {
  const { setPlot } = useWidgetPlotContext();

  const metricUnit = useMetricUnit();

  const onChange = useCallback(
    (e: React.ChangeEvent<HTMLSelectElement>) => {
      const unit = toMetricType(e.currentTarget.value);
      setPlot((p) => {
        p.metricUnit = unit ?? undefined;
      });
    },
    [setPlot]
  );
  return (
    <select className={cn('form-select', className)} value={metricUnit} onChange={onChange}>
      {METRIC_TYPE_KEYS.map((unit_type) => (
        <option key={unit_type} value={unit_type}>
          {METRIC_TYPE_DESCRIPTION_SELECTOR[unit_type]}
        </option>
      ))}
    </select>
  );
});
