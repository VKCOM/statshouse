// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback, useMemo } from 'react';
import { METRIC_TYPE, METRIC_TYPE_DESCRIPTION, type MetricType, toMetricType } from 'api/enum';
import cn from 'classnames';
import { getMetricType } from 'common/formatByMetricType';
import { type PlotKey } from 'url2';
import { useStatsHouseShallow } from 'store2';

const METRIC_TYPE_KEYS: MetricType[] = ['null', ...Object.values(METRIC_TYPE)] as MetricType[];
const METRIC_TYPE_DESCRIPTION_SELECTOR = {
  null: 'infer unit',
  ...METRIC_TYPE_DESCRIPTION,
};

export type PlotControlUnitProps = {
  className?: string;
  plotKey: PlotKey;
};
export function _PlotControlUnit({ className, plotKey }: PlotControlUnitProps) {
  const { metricUnitParam, what, metaMetricType, setPlot } = useStatsHouseShallow((s) => ({
    metricUnitParam: s.params.plots[plotKey]?.metricUnit,
    what: s.plotsData[plotKey]?.whats ?? s.params.plots[plotKey]?.what,
    metaMetricType:
      s.plotsData[plotKey]?.metricUnit ??
      s.metricMeta[s.plotsData[plotKey]?.metricName ?? s.params.plots[plotKey]?.metricName ?? '']?.metric_type,
    setPlot: s.setPlot,
  }));

  const metricUnit = useMemo(() => {
    if (metricUnitParam != null) {
      return metricUnitParam;
    }
    return getMetricType(what, metaMetricType);
  }, [metaMetricType, metricUnitParam, what]);

  const onChange = useCallback(
    (e: React.ChangeEvent<HTMLSelectElement>) => {
      const unit = toMetricType(e.currentTarget.value);
      setPlot(plotKey, (p) => {
        p.metricUnit = unit ?? undefined;
      });
    },
    [plotKey, setPlot]
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
}

export const PlotControlUnit = memo(_PlotControlUnit);
