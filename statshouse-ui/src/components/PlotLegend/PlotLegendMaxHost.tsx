import React, { memo, useMemo } from 'react';
import { Select } from '../Select';
import { selectorPlotsDataByIndex, useStore } from '../../store';

type PlotLegendMaxHostProps = {
  value: string;
  placeholder: string;
  indexPlot: number;
  idx: number;
};

function _PlotLegendMaxHost({ value, placeholder, indexPlot, idx }: PlotLegendMaxHostProps) {
  const selectorPlotsData = useMemo(() => selectorPlotsDataByIndex.bind(undefined, indexPlot), [indexPlot]);

  const { maxHostLists } = useStore(selectorPlotsData);

  const options = useMemo(() => maxHostLists[idx - 1] ?? [], [idx, maxHostLists]);
  return (
    <Select
      className="form-control pt-0 pb-0 min-h-auto form-control-sm"
      classNameList="dropdown-menu"
      value={value}
      placeholder={placeholder}
      options={options}
      listOnlyOpen
      showCountItems
    />
  );
}

export const PlotLegendMaxHost = memo(_PlotLegendMaxHost);
