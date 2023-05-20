import { PlotParams } from '../common/plotQueryParams';
import { useMemo } from 'react';
import { ApiTableRowNormalize } from '../api/table';
import { selectorMetricsMetaByName, useStore } from '../store';
import { getEventTagColumns } from '../view/utils';

export type UseEventTagColumnReturn = {
  keyTag: keyof ApiTableRowNormalize;
  fullKeyTag: string;
  name: string;
  selected: boolean;
  disabled: boolean;
};

export function useEventTagColumns(plot: PlotParams, selectedOnly: boolean = false): UseEventTagColumnReturn[] {
  const selectorMetricsMeta = useMemo(
    () => selectorMetricsMetaByName.bind(undefined, plot.metricName),
    [plot.metricName]
  );
  const meta = useStore(selectorMetricsMeta);
  return useMemo(() => getEventTagColumns(plot, meta, selectedOnly), [meta, plot, selectedOnly]);
}
