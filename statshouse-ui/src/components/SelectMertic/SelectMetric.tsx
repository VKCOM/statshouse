import { Select, SelectOptionProps } from '../Select';
import React, { useCallback, useMemo } from 'react';
import { updateMetricsList, useMetricsListStore } from 'store/metricsList';
import cn from 'classnames';

export type SelectMetricProps = {
  value?: string;
  onChange?: (value?: string | string[]) => void;
  className?: string;
  placeholder?: string;
};
export function SelectMetric({ value, onChange, className, placeholder }: SelectMetricProps) {
  const { list, loading } = useMetricsListStore();
  const metricsOptions = useMemo<SelectOptionProps[]>(() => list.map(({ name }) => ({ name, value: name })), [list]);
  const onSearchMetrics = useCallback((values: SelectOptionProps[]) => {
    if (values.length === 0 && !useMetricsListStore.getState().loading) {
      updateMetricsList();
    }
  }, []);
  return (
    <Select
      value={value}
      placeholder={placeholder}
      showSelected={!placeholder}
      options={metricsOptions}
      onChange={onChange}
      valueToInput={true}
      className={cn('sh-select form-control', className)}
      classNameList="dropdown-menu"
      onSearch={onSearchMetrics}
      loading={loading}
    />
  );
}
