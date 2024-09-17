// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { PlotKey, VariableParams } from 'url2';
import { useStatsHouseShallow } from 'store2';
import { useMemo } from 'react';
import { filterVariableByPromQl } from 'store2/helpers/filterVariableByPromQl';

export function useVariablesPlotByPromQL(plotKey: PlotKey) {
  const { variables, orderVariables, plotPromQL } = useStatsHouseShallow(
    ({ params: { variables, orderVariables, plots } }) => ({
      variables,
      orderVariables,
      plotPromQL: plots[plotKey]?.promQL,
    })
  );
  const filter = useMemo(() => filterVariableByPromQl(plotPromQL), [plotPromQL]);
  return useMemo(
    () =>
      orderVariables.reduce((res, vK) => {
        if (filter(variables[vK])) {
          res.push(variables[vK]!);
        }
        return res;
      }, [] as VariableParams[]),
    [filter, orderVariables, variables]
  );
}
