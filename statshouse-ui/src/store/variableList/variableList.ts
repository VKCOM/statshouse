// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import { Store, useStore } from '../statshouse';
import {
  getTagDescription,
  isTagEnabled,
  isValidVariableName,
  loadAllMeta,
  promQLMetric,
  replaceVariable,
} from '../../view/utils';
import { apiMetricTagValuesFetch, MetricTagValueInfo } from '../../api/metricTagValues';
import { GET_PARAMS, METRIC_VALUE_BACKEND_VERSION, QueryWhat } from '../../api/enum';
import { globalSettings } from '../../common/settings';
import { filterParamsArr } from '../../view/api';
import { useErrorStore } from '../errors';
import { isNotNil } from '../../common/helpers';
import { MetricMetaTag } from '../../api/metric';
import { getEmptyVariableParams } from '../../common/getEmptyVariableParams';
import { VariableParams } from '../../url/queryParams';

export function getEmptyVariable(): VariableItem {
  return { list: [], updated: false, loaded: false, more: false, tagMeta: undefined };
}

export const emptyVariableItem: Readonly<VariableItem> = getEmptyVariable();

export type VariableItem = {
  list: MetricTagValueInfo[];
  updated: boolean;
  loaded: boolean;
  more: boolean;
  tagMeta?: MetricMetaTag;
};

export type VariableListStore = {
  variables: Record<string, VariableItem>;
  tags: Record<string, Record<string, VariableItem>>;
};

export const useVariableListStore = create<VariableListStore>()(
  immer((setState, getState) => {
    let prevParams = useStore.getState().params;
    let metricsMeta = useStore.getState().metricsMeta;
    useStore.subscribe((state) => {
      if (prevParams !== state.params) {
        prevParams = state.params;
        updateVariables(state);
        updateTags(state);
      }
      if (metricsMeta !== state.metricsMeta) {
        metricsMeta = state.metricsMeta;
        const variableItems = getState().variables;
        state.params.variables.forEach((variable) => {
          if (!variableItems[variable.name].tagMeta) {
            variable.link.forEach(([iPlot, iTag]) => {
              if (iPlot != null && iTag != null) {
                const meta = metricsMeta[state.params.plots[iPlot].metricName];
                setState((variableState) => {
                  if (variableState.variables[variable.name]) {
                    variableState.variables[variable.name].tagMeta = meta?.tags?.[iTag];
                  }
                });
              }
            });
          }
        });
      }
    });
    return {
      variables: {},
      tags: {},
    };
  })
);
export function updateTags(state: Store) {
  const indexPlot = state.params.tabNum;
  const updated: number[] = [];
  if (indexPlot > -1) {
    const tags = useVariableListStore.getState().tags;
    if (tags[indexPlot]) {
      Object.entries(tags[indexPlot]).forEach(([indexTag, tagInfo]) => {
        if (tagInfo.updated) {
          updated.push(+indexTag);
        }
      });
    }
  }
  updated.forEach((indexTag) => {
    updateTag(indexPlot, indexTag);
  });
}
export async function updateTag(indexPlot: number, indexTag: number) {
  useVariableListStore.setState((state) => {
    state.tags[indexPlot] ??= {};
    state.tags[indexPlot][indexTag] ??= getEmptyVariable();
    if (state.tags[indexPlot]?.[indexTag]) {
      state.tags[indexPlot][indexTag].loaded = true;
    }
  });
  const listTag = await loadTagList(indexPlot, indexTag);
  useVariableListStore.setState((state) => {
    if (state.tags[indexPlot]?.[indexTag]) {
      state.tags[indexPlot][indexTag].list = listTag?.values ?? [];
      state.tags[indexPlot][indexTag].more = listTag?.more ?? false;
      state.tags[indexPlot][indexTag].tagMeta = listTag?.tagMeta;
      state.tags[indexPlot][indexTag].loaded = false;
    }
  });
}

export function setUpdatedTag(indexPlot: number, indexTag: number | undefined, toggle: boolean) {
  if (indexTag == null) {
    return;
  }
  useVariableListStore.setState((state) => {
    state.tags[indexPlot] ??= {};
    state.tags[indexPlot][indexTag] ??= getEmptyVariable();
    state.tags[indexPlot][indexTag].updated = toggle;
  });
  if (toggle) {
    updateTag(indexPlot, indexTag);
  }
}
export function clearTags(indexPlot: number) {
  useVariableListStore.setState((state) => {
    delete state.tags[indexPlot];
  });
}

export function updateVariables(store: Store) {
  const update: VariableParams[] = [];
  useVariableListStore.setState((state) => {
    const variables: Record<string, VariableItem> = {};
    store.params.variables.forEach((variable) => {
      variables[variable.name] = state.variables[variable.name] ?? getEmptyVariable();
      if (variables[variable.name].updated) {
        update.push(variable);
      }
    });
    state.variables = variables;
  });
  update.forEach(updateVariable);
}

export async function updateVariable(variableParam: VariableParams) {
  useVariableListStore.setState((state) => {
    if (state.variables[variableParam.name]) {
      state.variables[variableParam.name].loaded = true;
    }
  });
  const lists = await loadValuableList(variableParam);
  const more = lists.some((l) => l.more);
  const tagMeta = lists[0]?.tagMeta;
  const list = Object.values(
    lists
      .flatMap((l) => l.values)
      .reduce((res, t) => {
        if (res[t.value]) {
          res[t.value].count += t.count;
        } else {
          res[t.value] = t;
        }
        return res;
      }, {} as Record<string, MetricTagValueInfo>)
  );
  useVariableListStore.setState((state) => {
    if (state.variables[variableParam.name]) {
      state.variables[variableParam.name].list = list;
      state.variables[variableParam.name].loaded = false;
      state.variables[variableParam.name].more = more;
      state.variables[variableParam.name].tagMeta = tagMeta;
    }
  });
}

export async function loadValuableList(variableParam: VariableParams) {
  const lists = await Promise.all(
    variableParam.link.map(async ([indexPlot, indexTag]) => await loadTagList(indexPlot, indexTag))
  );
  return lists.filter(isNotNil);
}

export async function loadTagList(indexPlot: number | null, indexTag: number | null, limit = 20000) {
  const store = useStore.getState();
  if (indexPlot == null || indexTag == null || store.params.plots[indexPlot]?.metricName === promQLMetric) {
    return undefined;
  }
  const tagKey = indexTag === -1 ? '_s' : `${indexTag}`;
  const plot = replaceVariable(indexPlot, store.params.plots[indexPlot], store.params.variables);
  const tagID = indexTag === -1 ? 'skey' : `key${indexTag}`;
  const otherFilterIn = { ...plot.filterIn };
  delete otherFilterIn[tagID];
  const otherFilterNotIn = { ...plot.filterNotIn };
  delete otherFilterNotIn[tagID];
  const requestKey = `variable_${indexPlot}-${plot.metricName}`;
  await store.loadMetricsMeta(plot.metricName);
  const tagMeta = useStore.getState().metricsMeta[plot.metricName]?.tags?.[indexTag];
  const { response, error } = await apiMetricTagValuesFetch(
    {
      [GET_PARAMS.metricName]: plot.metricName,
      [GET_PARAMS.metricTagID]: tagKey,
      [GET_PARAMS.version]:
        globalSettings.disabled_v1 || plot.useV2 ? METRIC_VALUE_BACKEND_VERSION.v2 : METRIC_VALUE_BACKEND_VERSION.v1,
      [GET_PARAMS.numResults]: limit.toString(),
      [GET_PARAMS.fromTime]: store.timeRange.from.toString(),
      [GET_PARAMS.toTime]: (store.timeRange.to + 1).toString(),
      [GET_PARAMS.metricFilter]: filterParamsArr(otherFilterIn, otherFilterNotIn),
      [GET_PARAMS.metricWhat]: plot.what.slice() as QueryWhat[],
    },
    requestKey
  );
  if (response) {
    return {
      values: response.data.tag_values.slice(),
      more: response.data.tag_values_more,
      tagMeta,
    };
  }
  if (error) {
    useErrorStore.getState().addError(error);
  }
  return undefined;
}

export function setUpdatedVariable(nameVariable: string | undefined, toggle: boolean) {
  if (nameVariable == null) {
    return;
  }
  useVariableListStore.setState((state) => {
    state.variables[nameVariable] ??= getEmptyVariable();
    state.variables[nameVariable].updated = toggle;
  });
  updateVariables(useStore.getState());
}

export async function getAutoSearchSyncFilter(startIndex: number = 0) {
  const { params, loadMetricsMeta } = useStore.getState();
  await loadAllMeta(params, loadMetricsMeta);
  const { metricsMeta } = useStore.getState();
  const variablesLink: Record<string, [number, number][]> = {};
  params.plots.forEach(({ metricName }, indexPlot) => {
    if (metricName === promQLMetric) {
      return;
    }
    const meta = metricsMeta[metricName];
    if (!meta) {
      return;
    }
    meta.tags?.forEach((tag, indexTag) => {
      if (isTagEnabled(meta, indexTag)) {
        const tagName = getTagDescription(meta, indexTag);
        variablesLink[tagName] ??= [];
        variablesLink[tagName].push([indexPlot, indexTag]);
      }
    });
    if (isTagEnabled(meta, -1)) {
      const tagName = getTagDescription(meta, -1);
      variablesLink[tagName] ??= [];
      variablesLink[tagName].push([indexPlot, -1]);
    }
  });
  const addVariables: VariableParams[] = Object.entries(variablesLink)
    .filter(([, link]) => link.length > 1)
    .map(([description, link], index) => {
      const name = isValidVariableName(description)
        ? description
        : `${GET_PARAMS.variableNamePrefix}${startIndex + index}`;
      return {
        ...getEmptyVariableParams(),
        name,
        description: description === name ? '' : description,
        link,
      };
    });
  return addVariables;
}

updateVariables(useStore.getState());
updateTags(useStore.getState());
