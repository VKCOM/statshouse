import React, { useCallback, useMemo } from 'react';
import cn from 'classnames';
import { isMetricAggregation, METRIC_AGGREGATION, METRIC_AGGREGATION_DESCRIPTION } from '../../api/enum';

export type PlotControlAggregationProps = {
  value: number;
  onChange: (value: number) => void;
  className?: string;
};

const aggregationList = Object.values(METRIC_AGGREGATION).map((value) => ({
  value,
  description: METRIC_AGGREGATION_DESCRIPTION[value],
}));

export function PlotControlAggregation({ value, className, onChange }: PlotControlAggregationProps) {
  const onChangeAgg = useCallback(
    (e: React.ChangeEvent<HTMLSelectElement>) => {
      const customAgg = parseInt(e.currentTarget.value);
      onChange?.(customAgg);
    },
    [onChange]
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
