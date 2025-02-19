// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { apiMetricTagValuesFetch, type MetricTagValueInfo } from '@/api/metricTagValues';
import {
  GET_PARAMS,
  isTagKey,
  METRIC_VALUE_BACKEND_VERSION,
  QUERY_WHAT,
  type QueryWhat,
  type TagKey,
} from '@/api/enum';
import { globalSettings } from '@/common/settings';
import { filterParamsArr } from '@/view/api';
import { deepClone, isNotNil, toNumber } from '@/common/helpers';
import { getMetricMeta, loadMetricMeta, MetricMetaTag } from '@/api/metric';
import { createStore } from '../createStore';

import { produce } from 'immer';
import { useErrorStore } from '@/store2/errors';
import { replaceVariable } from './replaceVariable';
import {
  getNewVariable,
  type PlotKey,
  PlotParams,
  promQLMetric,
  VariableKey,
  type VariableParams,
  type VariableParamsSource,
} from '@/url2';
import { type StatsHouseStore, useStatsHouse } from '@/store2';
import { ExtendedError } from '@/api/api';

export type VariableItem = {
  list: MetricTagValueInfo[];
  updated: boolean;
  loaded: boolean;
  more: boolean;
  tagMeta?: MetricMetaTag;
  keyLastRequest: string;
};

export type VariableListStore = {
  variables: Partial<Record<string, VariableItem>>;
  tags: Partial<Record<PlotKey, Record<TagKey, VariableItem>>>;
  source: Partial<Record<string, Record<TagKey, VariableItem>>>;
};

export const useVariableListStore = createStore<VariableListStore>(() => ({
  variables: {},
  tags: {},
  source: {},
}));

useStatsHouse.subscribe((state, prevState) => {
  if (prevState.params.dashboardId !== state.params.dashboardId) {
    clearTagsAll();
  } else if (prevState.params.plots !== state.params.plots) {
    prevState.params.orderPlot.forEach((plotKey) => {
      if (
        !state.params.plots[plotKey] ||
        state.params.plots[plotKey]?.metricName !== prevState.params.plots[plotKey]?.metricName
      ) {
        clearTags(plotKey);
      }
    });
  }
  if (
    prevState.params.variables !== state.params.variables ||
    prevState.params.orderVariables !== state.params.orderVariables
  ) {
    updateVariables(state);
  }
  if (prevState.params !== state.params) {
    updateTags(state);
  }
});

export function updateVariablesMetaInfo(
  orderVariables: VariableKey[],
  variables: Partial<Record<VariableKey, VariableParams>>,
  plots: Partial<Record<PlotKey, PlotParams>>
) {
  const variableItems = useVariableListStore.getState().variables;
  orderVariables.forEach((variableKey) => {
    const variable = variables[variableKey];
    if (variable && !variableItems[variable.name]?.tagMeta) {
      variable.link.forEach(([plotKey, tagKey]) => {
        const plot = plots[plotKey];
        if (plot) {
          const meta = getMetricMeta(plot.metricName);
          useVariableListStore.setState(
            produce((variableState) => {
              if (variableState.variables[variable.name]) {
                variableState.variables[variable.name].tagMeta = meta?.tags?.[toNumber(tagKey, -1)];
              }
            })
          );
        }
      });
    }
  });
}

export function updateTags(state: StatsHouseStore) {
  const plotKey = state.params.tabNum;
  const updated: TagKey[] = [];
  if (plotKey != null) {
    const tags = useVariableListStore.getState().tags;
    if (tags[plotKey]) {
      Object.entries(tags[plotKey] ?? {}).forEach(([indexTag, tagInfo]) => {
        if (tagInfo.updated && isTagKey(indexTag)) {
          updated.push(indexTag);
        }
      });
    }
    updated.forEach((indexTag) => {
      updateTag(plotKey, indexTag);
    });
  }
}
export async function updateTag(plotKey: PlotKey, tagKey: TagKey) {
  useVariableListStore.setState(
    produce((state) => {
      state.tags[plotKey] ??= {} as Record<TagKey, VariableItem>;
      state.tags[plotKey][tagKey] ??= getNewVariable();
      if (state.tags[plotKey]?.[tagKey]) {
        state.tags[plotKey][tagKey].loaded = true;
      }
    })
  );
  const listTag = await loadTagList(plotKey, tagKey);
  useVariableListStore.setState(
    produce((state) => {
      if (state.tags[plotKey]?.[tagKey]) {
        state.tags[plotKey][tagKey].list = listTag?.values ?? [];
        state.tags[plotKey][tagKey].more = listTag?.more ?? false;
        state.tags[plotKey][tagKey].tagMeta = listTag?.tagMeta;
        state.tags[plotKey][tagKey].loaded = false;
        state.tags[plotKey][tagKey].keyLastRequest = listTag?.keyLastRequest ?? '';
      }
    })
  );
}

export function setUpdatedTag(plotKey: PlotKey, tagKey: TagKey | undefined, toggle: boolean) {
  if (tagKey == null) {
    return;
  }
  useVariableListStore.setState(
    produce((state) => {
      state.tags[plotKey] ??= {} as Record<TagKey, VariableItem>;
      state.tags[plotKey][tagKey] ??= getNewVariable();
      state.tags[plotKey][tagKey].updated = toggle;
    })
  );
  if (toggle) {
    updateTag(plotKey, tagKey).then();
  }
}

export async function updateSource(variableParamSource: VariableParamsSource) {
  useVariableListStore.setState(
    produce((state) => {
      state.source[variableParamSource.metric] ??= {} as Record<TagKey, VariableItem>;
      state.source[variableParamSource.metric][variableParamSource.tag] ??= getNewVariable();
      if (state.source[variableParamSource.metric]?.[variableParamSource.tag]) {
        state.source[variableParamSource.metric][variableParamSource.tag].loaded = true;
      }
    })
  );
  const listSource = await loadSourceList(variableParamSource);
  useVariableListStore.setState(
    produce((state) => {
      if (state.source[variableParamSource.metric]?.[variableParamSource.tag]) {
        state.source[variableParamSource.metric][variableParamSource.tag].list = listSource?.values ?? [];
        state.source[variableParamSource.metric][variableParamSource.tag].more = listSource?.more ?? false;
        state.source[variableParamSource.metric][variableParamSource.tag].tagMeta = listSource?.tagMeta;
        state.source[variableParamSource.metric][variableParamSource.tag].loaded = false;
        state.source[variableParamSource.metric][variableParamSource.tag].keyLastRequest =
          listSource?.keyLastRequest ?? '';
      }
    })
  );
}

export function setUpdatedSource(variableParamSource: VariableParamsSource, toggle: boolean) {
  if (variableParamSource.tag == null) {
    return;
  }
  useVariableListStore.setState(
    produce((state) => {
      state.source[variableParamSource.metric] ??= {} as Record<TagKey, VariableItem>;
      state.source[variableParamSource.metric][variableParamSource.tag] ??= getNewVariable();
      state.source[variableParamSource.metric][variableParamSource.tag].updated = toggle;
    })
  );
  if (toggle) {
    updateSource(variableParamSource);
  }
}

export function clearTags(plotKey: PlotKey) {
  useVariableListStore.setState(
    produce<VariableListStore>((state) => {
      delete state.tags[plotKey];
    })
  );
}

export function clearTagsAll() {
  useVariableListStore.setState(
    produce((state) => {
      state.tags = {};
    })
  );
}

export function updateVariables(store: StatsHouseStore) {
  const update: VariableParams[] = [];
  useVariableListStore.setState(
    produce((state) => {
      const variables: Record<string, VariableItem> = {};
      store.params.orderVariables.forEach((variableKey) => {
        const variable = store.params.variables[variableKey];
        if (variable) {
          variables[variable.name] = state.variables[variable.name] ?? getNewVariable();
          if (variables[variable.name].updated) {
            update.push(variable);
          }
        }
      });
      state.variables = variables;
    })
  );
  update.forEach(updateVariable);
}

export async function updateVariable(variableParam: VariableParams) {
  useVariableListStore.setState(
    produce((state) => {
      if (state.variables[variableParam.name]) {
        state.variables[variableParam.name].loaded = true;
      }
    })
  );
  const [sources, lists] = await Promise.all([loadValuableSourceList(variableParam), loadValuableList(variableParam)]);

  useVariableListStore.setState(
    produce((state) => {
      lists.forEach((listTag) => {
        if (listTag) {
          const { plotKey, tagKey } = listTag;
          state.tags[plotKey] ??= {} as Record<TagKey, VariableItem>;
          state.tags[plotKey][tagKey] ??= getNewVariable();
          if (state.tags[plotKey]?.[tagKey]) {
            state.tags[plotKey][tagKey].list = deepClone(listTag?.values ?? []);
            state.tags[plotKey][tagKey].more = listTag?.more ?? false;
            state.tags[plotKey][tagKey].tagMeta = deepClone(listTag?.tagMeta);
            state.tags[plotKey][tagKey].loaded = false;
            state.tags[plotKey][tagKey].keyLastRequest = listTag?.keyLastRequest ?? '';
          }
        }
      });
      sources.forEach((listTag) => {
        if (listTag) {
          const { metricName, tagKey } = listTag;
          state.source[metricName] ??= {} as Record<TagKey, VariableItem>;
          state.source[metricName][tagKey] ??= getNewVariable();
          if (state.source[metricName]?.[tagKey]) {
            state.source[metricName][tagKey].list = deepClone(listTag?.values ?? []);
            state.source[metricName][tagKey].more = listTag?.more ?? false;
            state.source[metricName][tagKey].tagMeta = deepClone(listTag?.tagMeta);
            state.source[metricName][tagKey].loaded = false;
            state.source[metricName][tagKey].keyLastRequest = listTag?.keyLastRequest ?? '';
          }
        }
      });
    })
  );
  const more = lists.some((l) => l.more);
  const tagMeta = lists[0]?.tagMeta ?? sources[0]?.tagMeta;
  const list = Object.values(
    [...sources.flatMap((l) => l.values), ...lists.flatMap((l) => l.values)].reduce(
      (res, t) => {
        if (res[t.value]) {
          res[t.value].count += t.count;
        } else {
          res[t.value] = { ...t };
        }
        return res;
      },
      {} as Record<string, MetricTagValueInfo>
    )
  );
  useVariableListStore.setState(
    produce((state) => {
      if (state.variables[variableParam.name]) {
        state.variables[variableParam.name].list = list;
        state.variables[variableParam.name].loaded = false;
        state.variables[variableParam.name].more = more;
        state.variables[variableParam.name].tagMeta = tagMeta;
      }
    })
  );
}

export async function loadValuableList(variableParam: VariableParams) {
  const lists = await Promise.all(
    variableParam.link.map(async ([indexPlot, indexTag]) => await loadTagList(indexPlot, indexTag))
  );
  return lists.filter(isNotNil);
}

export async function loadTagList(plotKey: PlotKey, tagKey: TagKey, limit = 25000) {
  const store = useStatsHouse.getState();
  if (!store.params.plots[plotKey] || store.params.plots[plotKey]?.metricName === promQLMetric) {
    return undefined;
  }
  if (!tagKey) {
    return undefined;
  }
  const prevPlot = store.params.plots[plotKey];
  if (!prevPlot) {
    return undefined;
  }
  const plot = replaceVariable(plotKey, prevPlot, store.params.variables);
  const otherFilterIn = { ...plot.filterIn };
  delete otherFilterIn[tagKey];
  const otherFilterNotIn = { ...plot.filterNotIn };
  delete otherFilterNotIn[tagKey];
  const requestKey = `variable_${plotKey}-${plot.metricName}`;
  const meta = await loadMetricMeta(plot.metricName);
  const tagMeta = meta?.tags?.[toNumber(tagKey, -1)];
  const params = {
    [GET_PARAMS.metricName]: plot.metricName,
    [GET_PARAMS.metricTagID]: tagKey,
    [GET_PARAMS.version]: plot.backendVersion,
    [GET_PARAMS.numResults]: limit.toString(),
    [GET_PARAMS.fromTime]: (store.params.timeRange.from - 1).toString(),
    [GET_PARAMS.toTime]: (store.params.timeRange.to + 1).toString(),
    [GET_PARAMS.metricFilter]: filterParamsArr(otherFilterIn, otherFilterNotIn),
    [GET_PARAMS.metricWhat]: plot.what.slice() as QueryWhat[],
  };
  const keyLastRequest = JSON.stringify(params);
  const lastTag = useVariableListStore.getState().tags[plotKey]?.[tagKey];
  if (lastTag && lastTag.keyLastRequest === keyLastRequest) {
    return {
      plotKey,
      tagKey,
      keyLastRequest: lastTag.keyLastRequest,
      values: lastTag.list,
      more: lastTag.more,
      tagMeta: lastTag.tagMeta,
    };
  }
  const { response, error } = await apiMetricTagValuesFetch(params, requestKey);
  if (response) {
    return {
      plotKey,
      tagKey,
      keyLastRequest,
      values: response.data.tag_values.slice(),
      more: response.data.tag_values_more,
      tagMeta,
    };
  }
  if (error && error.status !== ExtendedError.ERROR_STATUS_ABORT) {
    useErrorStore.getState().addError(error);
  }
  return undefined;
}

export async function loadValuableSourceList(variableParam: VariableParams) {
  const lists = await Promise.all(
    Object.values(variableParam.source)
      .filter(isNotNil)
      .map(async (source) => await loadSourceList(source))
  );
  return lists.filter(isNotNil);
}

export async function loadSourceList(variableParamSource: VariableParamsSource, limit = 25000) {
  const store = useStatsHouse.getState();
  const useV2 = store.params.orderPlot.every(
    (pK) => store.params.plots[pK]?.backendVersion === METRIC_VALUE_BACKEND_VERSION.v2
  );

  if (!variableParamSource.metric || !variableParamSource.tag || !useV2) {
    return undefined;
  }

  const requestKey = `variable_source_${variableParamSource.metric}-${variableParamSource.tag}`;
  const otherFilterIn = { ...variableParamSource.filterIn };
  // delete otherFilterIn[variableParamSource.tag];
  const otherFilterNotIn = { ...variableParamSource.filterNotIn };
  // delete otherFilterNotIn[variableParamSource.tag];
  const params = {
    [GET_PARAMS.metricName]: variableParamSource.metric,
    [GET_PARAMS.metricTagID]: variableParamSource.tag,
    [GET_PARAMS.version]:
      globalSettings.disabled_v1 || useV2 ? METRIC_VALUE_BACKEND_VERSION.v2 : METRIC_VALUE_BACKEND_VERSION.v1,
    [GET_PARAMS.numResults]: limit.toString(),
    [GET_PARAMS.fromTime]: (store.params.timeRange.from - 1).toString(),
    [GET_PARAMS.toTime]: (store.params.timeRange.to + 1).toString(),
    [GET_PARAMS.metricFilter]: filterParamsArr(otherFilterIn, otherFilterNotIn),
    [GET_PARAMS.metricWhat]: [QUERY_WHAT.count], //plot.what.slice() as QueryWhat[],
  };
  const keyLastRequest = JSON.stringify(params);
  const lastTag = useVariableListStore.getState().source[variableParamSource.metric]?.[variableParamSource.tag];
  const tagMeta = await loadMetricMeta(variableParamSource.metric);
  if (lastTag && lastTag.keyLastRequest === keyLastRequest) {
    return {
      metricName: variableParamSource.metric,
      tagKey: variableParamSource.tag,
      keyLastRequest: lastTag.keyLastRequest,
      values: lastTag.list,
      more: lastTag.more,
      tagMeta: lastTag.tagMeta,
    };
  }
  const { response, error } = await apiMetricTagValuesFetch(params, requestKey);
  if (response) {
    return {
      metricName: variableParamSource.metric,
      tagKey: variableParamSource.tag,
      keyLastRequest,
      values: response.data.tag_values.slice(),
      more: response.data.tag_values_more,
      tagMeta,
    };
  }
  if (error && error.status !== ExtendedError.ERROR_STATUS_ABORT) {
    useErrorStore.getState().addError(error);
  }
  return undefined;
}

export function setUpdatedVariable(nameVariable: string | undefined, toggle: boolean) {
  if (nameVariable == null) {
    return;
  }
  useVariableListStore.setState(
    produce((state) => {
      state.variables[nameVariable] ??= getNewVariable();
      state.variables[nameVariable].updated = toggle;
    })
  );
  updateVariables(useStatsHouse.getState());
}

updateVariables(useStatsHouse.getState());
updateTags(useStatsHouse.getState());
