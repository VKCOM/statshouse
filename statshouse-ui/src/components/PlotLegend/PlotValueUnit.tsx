import React from 'react';
import { METRIC_TYPE, MetricType } from '../../api/enum';
import { formatByMetricType } from '../../common/formatByMetricType';
import { useMemo } from 'react';

export type PlotValueUnitProps = {
  unit: MetricType;
  value?: number | null;
};

export function PlotValueUnit({ unit, value }: PlotValueUnitProps) {
  const format = useMemo(() => formatByMetricType(unit), [unit]);
  if (unit === METRIC_TYPE.none && value != null) {
    return <span className="small text-secondary">{value}</span>;
  }
  return value != null ? (
    <span className="small text-secondary">
      {format(value)}&nbsp;({value})
    </span>
  ) : null;
}
