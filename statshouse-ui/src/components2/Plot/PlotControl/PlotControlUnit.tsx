import React, { memo, useCallback, useMemo } from 'react';
import { type PlotKey, setPlot, useMetricsStore, useUrlStore } from 'store2';
import { METRIC_TYPE, METRIC_TYPE_DESCRIPTION, type MetricType, toMetricType } from 'api/enum';
import cn from 'classnames';
import { getMetricType } from '../../../common/formatByMetricType';
import { useShallow } from 'zustand/react/shallow';

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
  const { metricUnitParam, what, metricName } = useUrlStore(
    useShallow((s) => ({
      metricUnitParam: s.params.plots[plotKey]?.metricUnit,
      what: s.params.plots[plotKey]?.what,
      metricName: s.params.plots[plotKey]?.metricName ?? '',
    }))
  );
  const metaMetricType = useMetricsStore((s) => s.meta[metricName]?.metric_type);

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
    [plotKey]
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
