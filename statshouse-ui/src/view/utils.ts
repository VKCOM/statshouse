// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { GET_PARAMS } from '@/api/enum';
import { metricTagValueInfo } from './api';
import { MetricMetaValue } from '@/api/metric';
import { produce } from 'immer';
import { isNotNil, toNumber, uniqueArray } from '@/common/helpers';
import { getEmptyVariableParams } from '@/common/getEmptyVariableParams';
import {
  isNotNilVariableLink,
  PlotKey,
  PlotParams,
  QueryParams,
  toKeyTag,
  toPlotKey,
  toTagKey,
  VariableParams,
  VariableParamsLink,
} from '@/url/queryParams';
import { globalSettings } from '@/common/settings';
import { promQLMetric } from './promQLMetric';
import { getTagDescription, isValidVariableName } from './utils2';

export const goldenRatio = 1.61803398875;

export function clamp(n: number, min: number, max: number): number {
  return Math.max(min, Math.min(n, max));
}

type apiResponse<T> = {
  data?: T;
  error?: string;
};

export class Error403 extends Error {}
export class ErrorSkip extends Error {}

export async function apiGet<T>(url: string, signal: AbortSignal, promptReloadOn401: boolean): Promise<T> {
  const resp = await fetch(url, { signal });
  if (promptReloadOn401 && resp.status === 401) {
    if (window.confirm("API server has returned '401 Unauthorized' code. Reload the page to authorize?")) {
      window.location.reload();
    }
  }
  if (globalSettings.skip_error_code.indexOf(resp.status) > -1) {
    const text = await resp.clone().text();
    throw new ErrorSkip(`${resp.status}: ${text.substring(0, 255)}`);
  }
  if (resp.headers.get('Content-Type') !== 'application/json') {
    const text = await resp.text();
    throw new Error(`${resp.status}: ${text.substring(0, 255)}`);
  }
  const json = (await resp.json()) as apiResponse<T>;
  if (resp.status === 403) {
    throw new Error403(`${resp.status}: ${json.error}`);
  }
  if (!resp.ok || json.error) {
    throw new Error(`${resp.status}: ${json.error}`);
  }
  return json.data!;
}

export async function apiPost<T>(
  url: string,
  data: unknown,
  signal: AbortSignal,
  promptReloadOn401: boolean
): Promise<T> {
  const resp = await fetch(url, {
    method: 'POST',
    headers:
      data instanceof FormData
        ? {}
        : {
            'Content-Type': 'application/json',
          },
    body: data instanceof FormData ? data : JSON.stringify(data),
    signal,
  });
  if (promptReloadOn401 && resp.status === 401) {
    if (window.confirm("API server has returned '401 Unauthorized' code. Reload the page to authorize?")) {
      window.location.reload();
    }
  }
  if (globalSettings.skip_error_code.indexOf(resp.status) > -1) {
    const text = await resp.clone().text();
    throw new ErrorSkip(`${resp.status}: ${text.substring(0, 255)}`);
  }
  if (resp.headers.get('Content-Type') !== 'application/json') {
    const text = await resp.text();
    throw new Error(`${resp.status}: ${text.substring(0, 255)}`);
  }
  const json = (await resp.json()) as apiResponse<T>;
  if (resp.status === 403) {
    throw new Error403(`${resp.status}: ${json.error}`);
  }
  if (!resp.ok || json.error) {
    throw new Error(`${resp.status}: ${json.error}`);
  }
  return json.data!;
}

export async function apiPut<T>(
  url: string,
  data: unknown,
  signal: AbortSignal,
  promptReloadOn401: boolean
): Promise<T> {
  const resp = await fetch(url, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(data),
    signal,
  });
  if (promptReloadOn401 && resp.status === 401) {
    if (window.confirm("API server has returned '401 Unauthorized' code. Reload the page to authorize?")) {
      window.location.reload();
    }
  }
  if (resp.headers.get('Content-Type') !== 'application/json') {
    const text = await resp.text();
    throw new Error(`${resp.status}: ${text.substring(0, 255)}`);
  }
  const json = (await resp.json()) as apiResponse<T>;
  if (!resp.ok || json.error) {
    throw new Error(`${resp.status}: ${json.error}`);
  }
  return json.data!;
}

export function readJSONLD<T>(type: string): T | null {
  const elems = document.querySelectorAll('script[type="application/ld+json"]');
  for (let i = 0, max = elems.length; i < max; i++) {
    try {
      const json = JSON.parse(elems[i].innerHTML);
      if (json['@type'] === type) {
        return json as T;
      }
    } catch (_) {}
  }
  return null;
}

export function minMax(data: (number | null)[][], minIx: number = 0, maxIx?: number): [number | null, number | null] {
  let min = null;
  let max = null;
  for (let i = 0; i < data.length; i++) {
    const di = data[i];
    const k = maxIx !== undefined ? maxIx : di.length - 1;
    for (let j = minIx; j <= k && j < di.length; j++) {
      const v = di[j];
      if (max === null || (v !== null && v > max)) {
        max = v;
      }
      if (min === null || (v !== null && v < min)) {
        min = v;
      }
    }
  }
  return [min, max];
}

export function range0(r: [number | null, number | null]): [number | null, number | null] {
  let [min, max] = r;
  if (min !== null && min > 0) {
    min = 0;
  }
  return [min, max];
}

export function normalizeTagValues(values: readonly metricTagValueInfo[], sortByCount: boolean): metricTagValueInfo[] {
  const copy = [...values];
  if (sortByCount) {
    copy.sort((a, b) => (a.count > b.count ? -1 : a.count < b.count ? 1 : a.value.localeCompare(b.value)));
  } else {
    copy.sort((a, b) => a.value.localeCompare(b.value) || (a.count > b.count ? -1 : a.count < b.count ? 1 : 0));
  }
  const totalCount = copy.reduce((acc, v) => acc + v.count, 0);
  return copy.map((v) => ({ value: v.value, count: v.count / totalCount }));
}

export function sortByKey<T = unknown>(key: string, a: Record<string, T>, b: Record<string, T>) {
  return a[key] > b[key] ? 1 : a[key] < b[key] ? -1 : 0;
}

/**
 * replace filter value by variable
 *
 * @param plotKey
 * @param plot
 * @param variables
 */
export function replaceVariable(plotKey: PlotKey, plot: PlotParams, variables: VariableParams[]): PlotParams {
  return produce(plot, (p) => {
    variables.forEach(({ link, values, args }) => {
      const [, tagKey] = link.find(([iPlot]) => iPlot === plotKey) ?? [];
      if (tagKey == null) {
        return;
      }
      if (tagKey) {
        const ind = p.groupBy.indexOf(tagKey);
        if (args.groupBy) {
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
        if (args.negative) {
          p.filterNotIn[tagKey] = values.slice();
        } else {
          p.filterIn[tagKey] = values.slice();
        }
      }
    });
  });
}

export function getAutoNamStartIndex(variables: VariableParams[]): number {
  let maxIndex = 0;
  variables.forEach(({ name }) => {
    if (name.indexOf(GET_PARAMS.variableNamePrefix) === 0) {
      const index = +name.slice(GET_PARAMS.variableNamePrefix.length);
      if (!isNaN(index) && index > maxIndex) {
        maxIndex = index;
      }
    }
  });
  return maxIndex + 1;
}

export async function loadAllMeta(params: QueryParams, loadMetricsMeta: (metricName: string) => Promise<void>) {
  await Promise.all(
    params.plots.map(({ metricName }) =>
      metricName === promQLMetric ? Promise.resolve() : loadMetricsMeta(metricName)
    )
  );
  return;
}

export function tagSyncToVariableConvert(
  params: QueryParams,
  metricsMeta: Record<string, MetricMetaValue>
): QueryParams {
  return produce(params, (p) => {
    const startIndex = getAutoNamStartIndex(p.variables);
    const addVariables: VariableParams[] = p.tagSync
      .map((group, index) => {
        const link: VariableParamsLink[] = [];
        group.forEach((iTag, iPlot) => {
          const l = [toPlotKey(iPlot), toTagKey(iTag !== null ? toKeyTag(iTag) : iTag)];
          if (isNotNilVariableLink(l)) {
            link.push(l);
          }
        });
        if (link.length) {
          const indexPlot = toNumber(link[0][0]);
          if (indexPlot != null) {
            const description = getTagDescription(metricsMeta?.[params.plots[indexPlot]?.metricName], link[0][1]);
            const name = isValidVariableName(description)
              ? description
              : `${GET_PARAMS.variableNamePrefix}${startIndex + index}`;
            return {
              ...getEmptyVariableParams(),
              name,
              link,
              description: description === name ? '' : description,
            };
          }
        }
        return null;
      })
      .filter(isNotNil);
    p.tagSync = [];
    const updateParams = paramToVariable({ ...p, variables: addVariables });
    p.plots = updateParams.plots;
    p.variables = [...p.variables, ...updateParams.variables];
  });
}

export function paramToVariable(params: QueryParams): QueryParams {
  return produce(params, (p) => {
    p.variables = p.variables.map((variable) => {
      let groupBy = variable.args.groupBy;
      let negative = variable.args.negative;
      let values: string[] = variable.values;
      if (variable.link.length) {
        const [keyPlot0, keyTag0] = variable.link[0];
        const iPlot0 = toNumber(keyPlot0);
        if (iPlot0 != null) {
          if (keyTag0 != null) {
            groupBy = groupBy || p.plots[iPlot0]?.groupBy?.indexOf(keyTag0) > -1;
            negative = negative || !!p.plots[iPlot0]?.filterNotIn[keyTag0]?.length;
            values = uniqueArray([
              ...values,
              ...variable.link
                .map(([keyPlot, keyTag]) => {
                  const iPlot = toNumber(keyPlot);
                  if (iPlot != null) {
                    const values =
                      (negative ? p.plots[iPlot]?.filterNotIn[keyTag] : p.plots[iPlot]?.filterIn[keyTag]) ?? [];
                    delete p.plots[iPlot].filterIn[keyTag];
                    delete p.plots[iPlot].filterNotIn[keyTag];
                    p.plots[iPlot].groupBy = p.plots[iPlot].groupBy.filter((f) => f !== keyTag);
                    return values;
                  }
                  return [];
                })
                .flat(),
            ]);
          }
        }
      }
      return {
        ...variable,
        args: {
          ...variable.args,
          groupBy,
          negative,
        },
        values,
      };
    });
  });
}

export function plotLoadPrioritySort(params: QueryParams) {
  const plots = params.plots.map((plot, indexPlot) => ({ indexPlot, plot }));
  plots.sort((a, b) => {
    if (a.indexPlot === params.tabNum) {
      return -1;
    }
    if (b.indexPlot === params.tabNum) {
      return 1;
    }
    return a.indexPlot - b.indexPlot;
  });
  return plots;
}
