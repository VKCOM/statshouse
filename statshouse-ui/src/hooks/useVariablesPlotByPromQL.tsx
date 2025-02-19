// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { VariableParams } from '@/url2';
import { useMemo } from 'react';
import { filterVariableByPromQl } from '@/store2/helpers/filterVariableByPromQl';
import { useWidgetParamsContext, useWidgetPlotContext } from '@/contexts';

export function useVariablesPlotByPromQL() {
  const {
    params: { variables, orderVariables },
  } = useWidgetParamsContext();
  const {
    plot: { promQL },
  } = useWidgetPlotContext();

  const filter = useMemo(() => filterVariableByPromQl(promQL), [promQL]);
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
