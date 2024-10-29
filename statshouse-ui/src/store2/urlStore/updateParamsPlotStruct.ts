import { produce } from 'immer';
import type { ProduceUpdate } from '../helpers';
import { getNewMetric, GroupInfo, GroupKey, PlotKey, PlotParams, QueryParams, VariableKey, VariableParams } from 'url2';
import type { StatsHouseStore } from '../statsHouseStore';
import type { TagKey } from 'api/enum';
import { isNotNil, toNumberM } from 'common/helpers';
import { clonePlot } from '../../url2/clonePlot';
import { cloneGroup } from '../../url2/cloneGroup';
import { cloneVariable } from '../../url2/cloneVariable';

export type VariableLinks = { variableKey: VariableKey; tag: TagKey };

export type PlotStruct = {
  variables: {
    variableInfo: VariableParams;
  }[];
  groups: {
    groupInfo: GroupInfo;
    plots: { plotInfo: PlotParams; variableLinks: VariableLinks[] }[];
  }[];
  mapGroupIndex: Partial<Record<GroupKey, number>>;
  mapVariableIndex: Partial<Record<VariableKey, number>>;
  mapPlotIndex: Partial<Record<PlotKey, number>>;
  mapPlotToGroup: Partial<Record<PlotKey, GroupKey>>;
};

export function getPlotStruct(params: QueryParams): PlotStruct {
  const orderPlots = [...params.orderPlot];
  const mapGroupIndex: Partial<Record<GroupKey, number>> = {};
  const mapVariableIndex: Partial<Record<VariableKey, number>> = {};
  const mapPlotIndex: Partial<Record<PlotKey, number>> = {};
  const mapPlotToGroup: Partial<Record<PlotKey, GroupKey>> = {};
  const variableLinks: Partial<Record<PlotKey, VariableLinks[]>> = {};
  const variables = params.orderVariables.map((variableKey, variableIndex) => {
    mapVariableIndex[variableKey] = variableIndex;
    params.variables[variableKey]?.link.forEach(([pKey, tag]) => {
      (variableLinks[pKey] ??= []).push({ variableKey, tag });
    });
    return { variableInfo: cloneVariable(params.variables[variableKey]!) };
  });
  return {
    variables,
    groups: params.orderGroup.map((groupKey, groupIndex) => {
      mapGroupIndex[groupKey] = groupIndex;
      return {
        groupInfo: cloneGroup(params.groups[groupKey]!),
        plots: orderPlots.splice(0, params.groups[groupKey]!.count).map((plotKey, plotIndex) => {
          mapPlotIndex[plotKey] = plotIndex;
          mapPlotToGroup[plotKey] = groupKey;
          return {
            plotInfo: clonePlot(params.plots[plotKey] ?? { ...getNewMetric(), id: plotKey }),
            variableLinks: variableLinks[plotKey]?.map(({ variableKey, tag }) => ({ variableKey, tag })) ?? [],
          };
        }),
      };
    }),
    mapGroupIndex,
    mapVariableIndex,
    mapPlotIndex,
    mapPlotToGroup,
  };
}

export function updateParamsByPlotStruct(
  params: QueryParams,
  plotStruct: Pick<PlotStruct, 'groups' | 'variables'>
): QueryParams {
  const variables: Partial<Record<VariableKey, VariableParams>> = {};
  const plots: Partial<Record<PlotKey, PlotParams>> = {};
  const groups: Partial<Record<GroupKey, GroupInfo>> = {};
  const orderVariables = plotStruct.variables.map((v) => {
    const id = v.variableInfo.id || getNextVariableKey(params);
    variables[id] = { ...v.variableInfo, id, link: [] };
    return id;
  });
  const orderPlot: PlotKey[] = [];
  const orderGroup: GroupKey[] = plotStruct.groups.map((g) => {
    const groupId = g.groupInfo.id || getNextGroupKey(params);
    orderPlot.push(
      ...g.plots.map((p) => {
        const id = p.plotInfo.id || getNextPlotKey(params);
        plots[id] = { ...p.plotInfo, id };
        p.variableLinks.forEach((vl) => {
          variables[vl.variableKey]?.link.push([id, vl.tag]);
        });
        return id;
      })
    );
    groups[groupId] = { ...g.groupInfo, id: groupId, count: g.plots.length };
    return groupId;
  });
  orderVariables.forEach((vId) => {
    variables[vId]?.link.sort((a, b) => +a[0] - +b[0]);
  });
  return {
    ...params,
    orderVariables,
    variables,
    orderPlot,
    plots,
    orderGroup,
    groups,
  };
}

export function updateParamsPlotStruct(next: ProduceUpdate<PlotStruct>): ProduceUpdate<StatsHouseStore> {
  return (s) => {
    const plotStruct = getPlotStruct(s.params);
    s.params = updateParamsByPlotStruct(s.params, produce(plotStruct, next));
  };
}
export function updateQueryParamsPlotStruct(next: ProduceUpdate<PlotStruct>): ProduceUpdate<QueryParams> {
  return (p) => {
    const plotStruct = getPlotStruct(p);
    return updateParamsByPlotStruct(p, produce(plotStruct, next));
  };
}

export function getNextVariableKey(params: Pick<QueryParams, 'orderVariables'>): VariableKey {
  return (Math.max(-1, ...params.orderVariables.map(toNumberM).filter(isNotNil)) + 1).toString();
}

export function getNextGroupKey(params: Pick<QueryParams, 'orderGroup'>): GroupKey {
  return (Math.max(-1, ...params.orderGroup.map(toNumberM).filter(isNotNil)) + 1).toString();
}

export function getNextPlotKey(params: Pick<QueryParams, 'orderPlot'>): GroupKey {
  return (Math.max(-1, ...params.orderPlot.map(toNumberM).filter(isNotNil)) + 1).toString();
}

export function getNextVariableSourceKey(params: Pick<VariableParams, 'sourceOrder'>): VariableKey {
  return (Math.max(-1, ...params.sourceOrder.map(toNumberM).filter(isNotNil)) + 1).toString();
}
