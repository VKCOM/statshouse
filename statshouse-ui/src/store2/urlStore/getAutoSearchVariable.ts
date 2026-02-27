// Copyright 2026 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { StatsHouseStore } from '@/store2';
import { getNewVariable, promQLMetric, type QueryParams, type VariableParamsLink } from '@/url2';
import { GET_PARAMS, TAG_KEY, TagKey, toTagKey } from '@/api/enum';
import { produce } from 'immer';
import { getNextVariableKey } from './updateParamsPlotStruct';
import { clearOuterInfo, getTagDescription, isTagEnabled, isValidVariableName } from '@/view/utils2';
import { loadMetricMeta } from '@/api/metric';
import { selectorOrderPlot } from '@/store2/selectors';
import { isNotNil } from '@/common/helpers';
import { MetricMeta } from '@/store2/metricsMetaStore';

export async function getAutoSearchVariable(
  getState: () => StatsHouseStore
): Promise<Pick<QueryParams, 'variables' | 'orderVariables'>> {
  const {
    params: { plots, variables, orderVariables },
  } = getState();
  const orderPlot = selectorOrderPlot(getState());
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

export async function getMetricsMeta(metrics: string[]) {
  return (await Promise.all(metrics.map((metricName) => loadMetricMeta(metricName)))).filter(isNotNil);
}

export type VariableMetricPair = {
  name: string;
  count: number;
  links: {
    metricName: string;
    tagKey: TagKey;
  }[];
};

export function findPair(metricsMeta: MetricMeta[]) {
  return Object.values(
    metricsMeta.reduce(
      (res, meta) => {
        meta.tags?.forEach((tag, indexTag) => {
          const tagKey = toTagKey(indexTag);
          if (tagKey && isTagEnabled(meta, tagKey)) {
            if (tag.name) {
              res[tag.name] ??= { name: tag.name, count: 0, links: [] };
              res[tag.name].links.push({ metricName: meta.name, tagKey });
              res[tag.name].count++;
            }
            const description = clearOuterInfo(tag.description);
            if (description && tag.name !== description) {
              res[description] ??= { name: description, count: 0, links: [] };
              res[description].links.push({ metricName: meta.name, tagKey });
              res[description].count++;
            }
          }
        });
        if (isTagEnabled(meta, TAG_KEY._s)) {
          if (meta.string_top_name) {
            res[meta.string_top_name] ??= { name: meta.string_top_name, count: 0, links: [] };
            res[meta.string_top_name].links.push({ metricName: meta.name, tagKey: TAG_KEY._s });
            res[meta.string_top_name].count++;
          }
          const description = clearOuterInfo(meta.string_top_description);
          if (description && meta.string_top_name !== description) {
            res[description] ??= { name: description, count: 0, links: [] };
            res[description].links.push({ metricName: meta.name, tagKey: TAG_KEY._s });
            res[description].count++;
          }
        }
        return res;
      },
      {} as Record<VariableMetricPair['name'], VariableMetricPair>
    )
  ).sort((a, b) => b.links.length - a.links.length);
}
