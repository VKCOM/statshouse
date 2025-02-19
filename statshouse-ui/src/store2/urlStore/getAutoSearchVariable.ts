// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { StatsHouseStore } from '../statsHouseStore';
import { getNewVariable, promQLMetric, type QueryParams, type VariableParamsLink } from '@/url2';
import { GET_PARAMS, TAG_KEY, toTagKey } from '@/api/enum';
import { produce } from 'immer';
import { getNextVariableKey } from './updateParamsPlotStruct';
import { getTagDescription, isTagEnabled, isValidVariableName } from '@/view/utils2';
import { loadMetricMeta } from '@/api/metric';

export async function getAutoSearchVariable(
  getState: () => StatsHouseStore
): Promise<Pick<QueryParams, 'variables' | 'orderVariables'>> {
  const {
    params: { orderPlot, plots, variables, orderVariables },
  } = getState();
  const metricsMeta = await Promise.all(
    orderPlot.map((plotKey) => {
      const metricName = plots[plotKey]?.metricName;
      if (metricName && metricName !== promQLMetric) {
        return loadMetricMeta(metricName);
      }
      return Promise.resolve();
    })
  );
  // const { metricMeta } = getState();
  const variablesLink: Record<string, VariableParamsLink[]> = {};
  orderPlot.forEach((plotKey, indexPlot) => {
    const plot = plots[plotKey];
    if (!plot || plot.metricName === promQLMetric) {
      return;
    }
    const meta = metricsMeta[indexPlot];
    if (!meta) {
      return;
    }
    meta.tags?.forEach((_, indexTag) => {
      const tagKey = toTagKey(indexTag);
      if (tagKey && isTagEnabled(meta, tagKey)) {
        const tagName = getTagDescription(meta, indexTag);
        variablesLink[tagName] ??= [];
        variablesLink[tagName].push([plotKey, tagKey]);
      }
    });
    if (isTagEnabled(meta, TAG_KEY._s)) {
      const tagName = getTagDescription(meta, TAG_KEY._s);
      variablesLink[tagName] ??= [];
      variablesLink[tagName].push([plotKey, TAG_KEY._s]);
    }
  });
  return produce({ variables, orderVariables }, (v) => {
    Object.entries(variablesLink).forEach(([description, link]) => {
      if (link.length > 1) {
        const id = getNextVariableKey(v);
        const name = isValidVariableName(description) ? description : `${GET_PARAMS.variableNamePrefix}${id}`;
        v.variables[id] = {
          ...getNewVariable(),
          id,
          name,
          description: description === name ? '' : description,
          link,
        };
        v.orderVariables.push(id);
      }
    });
  });
}
