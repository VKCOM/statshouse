// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { PlotKey, QueryParams } from '@/url2';
import { PlotVariablesLink } from './plotsInfoStore';

export function updatePlotVariablesLink(params: QueryParams) {
  const plotVariablesLink: Partial<Record<PlotKey, PlotVariablesLink>> = {};
  Object.values(params.variables).forEach((variable) => {
    variable?.link.forEach(([plotKey, tagKey]) => {
      const p = (plotVariablesLink[plotKey] ??= {});
      p[tagKey] = { variableKey: variable.id, variableName: variable.name };
    });
  });
  return plotVariablesLink;
}
