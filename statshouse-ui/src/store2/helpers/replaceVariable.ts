// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { PlotKey, PlotParams, VariableParams } from '@/url2';
import { produce } from 'immer';
import { isNotNil } from '@/common/helpers';

/**
 * replace filter value by variable
 *
 * @param plotKey
 * @param plot
 * @param variables
 */
export function replaceVariable(
  plotKey: PlotKey,
  plot: PlotParams,
  variables: Partial<Record<string, VariableParams>>
): PlotParams {
  return produce(plot, (p) => {
    Object.values(variables)
      .filter(isNotNil)
      .forEach(({ link, values, groupBy, negative }) => {
        const [, tagKey] = link.find(([iPlot]) => iPlot === plotKey) ?? [];
        if (tagKey) {
          const ind = p.groupBy.indexOf(tagKey);
          if (groupBy) {
            if (ind === -1) {
              p.groupBy.push(tagKey);
            }
          } else {
            if (ind > -1) {
              p.groupBy.splice(ind, 1);
            }
          }
          delete p.filterIn[tagKey];
          delete p.filterNotIn[tagKey];
          if (negative) {
            p.filterNotIn[tagKey] = values.slice();
          } else {
            p.filterIn[tagKey] = values.slice();
          }
        }
      });
  });
}
