// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback } from 'react';
import { SelectMetric } from 'components/SelectMertic';
import { useStatsHouseShallow } from 'store2';
import { type PlotKey } from 'url2';

export type PlotControlMetricNameProps = {
  plotKey: PlotKey;
};
export function _PlotControlMetricName({ plotKey }: PlotControlMetricNameProps) {
  const { metricName, setPlot, removeVariableLinkByPlotKey } = useStatsHouseShallow(
    ({ params: { plots }, setPlot, removeVariableLinkByPlotKey }) => ({
      metricName: plots[plotKey]?.metricName,
      setPlot,
      removeVariableLinkByPlotKey,
    })
  );
  const onChange = useCallback(
    (value?: string | string[]) => {
      if (typeof value !== 'string') {
        return;
      }
      setPlot(plotKey, (p) => {
        p.metricName = value;
        p.customName = '';
        p.groupBy = [];
        p.filterIn = {};
        p.filterNotIn = {};
        p.customDescription = '';
      });
      removeVariableLinkByPlotKey(plotKey);
    },
    [plotKey, removeVariableLinkByPlotKey, setPlot]
  );
  return <SelectMetric value={metricName} onChange={onChange} />;
}

export const PlotControlMetricName = memo(_PlotControlMetricName);
