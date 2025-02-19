// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo, useCallback } from 'react';
import { SelectMetric } from '@/components/SelectMertic';
import { useMetricName } from '@/hooks/useMetricName';
import { useWidgetParamsContext, useWidgetPlotContext } from '@/contexts';
import { filterVariableByPlot } from '@/store2/helpers/filterVariableByPlot';

export const PlotControlMetricName = memo(function PlotControlMetricName() {
  const {
    params: { variables, orderVariables },
    setParams,
  } = useWidgetParamsContext();
  const { plot, setPlot } = useWidgetPlotContext();
  const metricName = useMetricName(true);

  const removeVariableLink = useCallback(() => {
    const plotFilter = filterVariableByPlot(plot);
    const variableKeys = orderVariables.filter((vK) => plotFilter(variables[vK]));
    if (variableKeys.length) {
      setParams((params) => {
        variableKeys.forEach((vK) => {
          const variable = params.variables[vK];
          if (variable) {
            variable.link = variable.link.filter(([pKey]) => pKey !== plot.id);
          }
        });
      }, true);
    }
  }, [orderVariables, plot, setParams, variables]);

  const onChange = useCallback(
    (value?: string | string[]) => {
      if (typeof value !== 'string') {
        return;
      }
      setPlot((p) => {
        p.metricName = value;
        p.customName = '';
        p.groupBy = [];
        p.filterIn = {};
        p.filterNotIn = {};
        p.customDescription = '';
      });
      removeVariableLink();
    },
    [removeVariableLink, setPlot]
  );
  return <SelectMetric value={metricName} onChange={onChange} />;
});
