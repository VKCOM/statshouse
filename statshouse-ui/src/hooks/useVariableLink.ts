// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useStatsHouse } from '@/store2';
import { useMemo } from 'react';
import type { PlotKey } from '@/url2';
import { PlotVariablesLink } from '@/store2/plotsInfoStore';
import { TagKey } from '@/api/enum';

export function useVariableLink(plotKey: PlotKey, tagKey: TagKey) {
  // todo: optimize multi use;
  const variables = useStatsHouse(({ params: { variables } }) => variables);
  return useMemo(() => {
    const plotVariablesLink: Partial<Record<PlotKey, PlotVariablesLink>> = {};
    Object.values(variables).forEach((variable) => {
      variable?.link.forEach(([plotKey, tagKey]) => {
        const p = (plotVariablesLink[plotKey] ??= {});
        p[tagKey] = { variableKey: variable.id, variableName: variable.name };
      });
    });
    return plotVariablesLink[plotKey]?.[tagKey];
  }, [plotKey, tagKey, variables]);
}
