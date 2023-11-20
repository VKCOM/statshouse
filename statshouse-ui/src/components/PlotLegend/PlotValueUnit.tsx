import React from 'react';
import { MetricType } from '../../api/enum';
import { formatByMetricType } from '../../common/formatByMetricType';
import { useMemo } from 'react';

export type PlotValueUnitProps = {
  unit: MetricType;
  value?: number | null;
};

export function PlotValueUnit({ unit, value }: PlotValueUnitProps) {
  const format = useMemo(() => formatByMetricType(unit), [unit]);
  return value != null ? <span className="small text-secondary">{format(value)}</span> : null;
}
