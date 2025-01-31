// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useMemo } from 'react';
import { ApiTableRowNormalize } from '../api/tableOld';
import { selectorMetricsMetaByName, useStore } from '../store';
import { getEventTagColumns } from '../view/utils';
import { PlotParams } from '../url/queryParams';

export type UseEventTagColumnReturn = {
  keyTag: keyof ApiTableRowNormalize;
  fullKeyTag: string;
  name: string;
  selected: boolean;
  disabled: boolean;
  hide: boolean;
};

export function useEventTagColumns(plot: PlotParams, selectedOnly: boolean = false): UseEventTagColumnReturn[] {
  const selectorMetricsMeta = useMemo(
    () => selectorMetricsMetaByName.bind(undefined, plot.metricName),
    [plot.metricName]
  );
  const meta = useStore(selectorMetricsMeta);
  return useMemo(() => getEventTagColumns(plot, meta, selectedOnly), [meta, plot, selectedOnly]);
}
