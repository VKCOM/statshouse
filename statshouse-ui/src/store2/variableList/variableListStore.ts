// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { getTagDescription, isTagEnabled, isValidVariableName } from '../../view/utils';
import { apiMetricTagValuesFetch, MetricTagValueInfo } from '../../api/metricTagValues';
import {
  GET_PARAMS,
  isTagKey,
  METRIC_VALUE_BACKEND_VERSION,
  QUERY_WHAT,
  QueryWhat,
  TAG_KEY,
  TagKey,
  toTagKey,
} from '../../api/enum';
import { globalSettings } from '../../common/settings';
import { filterParamsArr } from '../../view/api';
import { deepClone, isNotNil, toNumber } from '../../common/helpers';
import { MetricMetaTag } from '../../api/metric';
import { createStore } from '../createStore';
import {
  getNewVariable,
  PlotKey,
  promQLMetric,
  UrlStore,
  useUrlStore,
  VariableParams,
  VariableParamsLink,
  VariableParamsSource,
} from '../urlStore';
import { produce } from 'immer';
import { loadMetricMeta, MetricsStore, useMetricsStore } from '../metricsStore';
import { useErrorStore } from '../../store';
import { replaceVariable } from './replaceVariable';

// export function getEmptyVariable(): VariableItem {
//   return { list: [], updated: false, loaded: false, more: false, tagMeta: undefined, keyLastRequest: '' };
// }

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

export const useVariableListStore = createStore<VariableListStore>(
  (setState, getState) => ({
    variables: {},
    tags: {},
    source: {},
  }),
  'VariableListStore'
);

export function variableListStoreSubscribe(state: UrlStore, prevState: UrlStore) {
  if (prevState.params.dashboardId !== state.params.dashboardId || prevState.params.plots !== state.params.plots) {
    if (
      prevState.params.orderPlot.some(
        (plotKey) =>
          !state.params.plots[plotKey] ||
          state.params.plots[plotKey]?.metricName !== prevState.params.plots[plotKey]?.metricName
      )
    ) {
      clearTagsAll();
    }
  }
  if (prevState.params !== state.params) {
    updateVariables(state);
    updateTags(state);
  }
  // if (prevState.metricsMeta !== state.metricsMeta) {
  //   const variableItems = getState().variables;
  //   state.params.variables.forEach((variable) => {
  //     if (!variableItems[variable.name].tagMeta) {
  //       variable.link.forEach(([plotKey, tagKey]) => {
  //         const indexPlot = toNumber(plotKey);
  //         const indexTag = toIndexTag(tagKey);
  //         if (indexPlot != null && indexTag != null) {
  //           const meta = state.metricsMeta[state.params.plots[indexPlot].metricName];
  //           setState((variableState) => {
  //             if (variableState.variables[variable.name]) {
  //               variableState.variables[variable.name].tagMeta = meta?.tags?.[indexTag];
  //             }
  //           });
  //         }
  //       });
  //     }
  //   });
  // }
}
export function variableListStoreMetaSubscribe(state: MetricsStore, prevState: MetricsStore) {
  if (prevState.meta !== state.meta) {
    const variableItems = useVariableListStore.getState().variables;
    const { orderVariables, variables, plots } = useUrlStore.getState().params;
    orderVariables.forEach((variableKey) => {
      const variable = variables[variableKey];
      if (variable && !variableItems[variable.name]?.tagMeta) {
        variable.link.forEach(([plotKey, tagKey]) => {
          // const indexPlot = toNumber(plotKey);
          // const indexTag = toIndexTag(tagKey);
          // if (indexPlot != null && indexTag != null) {
          const plot = plots[plotKey];
          if (plot) {
            const meta = state.meta[plot.metricName];
            useVariableListStore.setState(
              produce((variableState) => {
                if (variableState.variables[variable.name]) {
                  variableState.variables[variable.name].tagMeta = meta?.tags?.[toNumber(tagKey, -1)];
                }
              })
            );
          }
          // }
        });
      }
    });
  }
}
useUrlStore.subscribe(variableListStoreSubscribe);
useMetricsStore.subscribe(variableListStoreMetaSubscribe);

export function updateTags(state: UrlStore) {
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

export function clearTags(indexPlot: number) {
  useVariableListStore.setState(
    produce((state) => {
      delete state.tags[indexPlot];
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

export function updateVariables(store: UrlStore) {
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
  // const indexPlot = toNumber(plotKey);
  // const indexTag = toIndexTag(tagKey);
  const store = useUrlStore.getState();
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
    [GET_PARAMS.version]:
      globalSettings.disabled_v1 || plot.useV2 ? METRIC_VALUE_BACKEND_VERSION.v2 : METRIC_VALUE_BACKEND_VERSION.v1,
    [GET_PARAMS.numResults]: limit.toString(),
    [GET_PARAMS.fromTime]: store.params.timeRange.from.toString(),
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
  if (error) {
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
  const store = useUrlStore.getState();
  const useV2 = store.params.orderPlot.every((pK) => store.params.plots[pK]?.useV2);
  // const tagKey = variableParamSource.tag;
  // const indexTag = toIndexTag(variableParamSource.tag);

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
    [GET_PARAMS.fromTime]: store.params.timeRange.from.toString(),
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
  if (error) {
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
  updateVariables(useUrlStore.getState());
}

export async function getAutoSearchSyncFilter(startIndex: number = 0) {
  const { params } = useUrlStore.getState();
  // await loadAllMeta(params, loadMetricsMeta);
  await Promise.all(
    params.orderPlot.map((plotKey) => {
      const metricName = params.plots[plotKey]?.metricName;
      if (metricName && metricName !== promQLMetric) {
        return loadMetricMeta(metricName);
      }
      return Promise.resolve();
    })
  );
  const { meta: metricsMeta } = useMetricsStore.getState();
  const variablesLink: Record<string, VariableParamsLink[]> = {};
  params.orderPlot.forEach((plotKey) => {
    const plot = params.plots[plotKey];
    if (!plot || plot.metricName === promQLMetric) {
      return;
    }
    const meta = metricsMeta[plot.metricName];
    if (!meta) {
      return;
    }
    meta.tags?.forEach((tag, indexTag) => {
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
  const addVariables: VariableParams[] = Object.entries(variablesLink)
    .filter(([, link]) => link.length > 1)
    .map(([description, link], index) => {
      const name = isValidVariableName(description)
        ? description
        : `${GET_PARAMS.variableNamePrefix}${startIndex + index}`;
      return {
        ...getNewVariable(),
        name,
        description: description === name ? '' : description,
        link,
      };
    });
  return addVariables;
}

updateVariables(useUrlStore.getState());
updateTags(useUrlStore.getState());
