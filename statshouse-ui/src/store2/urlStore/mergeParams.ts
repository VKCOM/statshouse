import { type GroupInfo, type PlotParams, type QueryParams, type VariableParams } from 'url2';
import { dequal } from 'dequal/lite';

export function mergeParams(target: QueryParams, value: QueryParams): QueryParams {
  if (target === value) {
    return target;
  }
  return {
    ...value,
    orderPlot: dequal(target.orderPlot, value.orderPlot) ? target.orderPlot : value.orderPlot,
    plots: value.orderPlot.reduce(
      (res, pK) => {
        res[pK] = dequal(target.plots[pK], value.plots[pK]) ? target.plots[pK] : value.plots[pK];
        return res;
      },
      {} as Partial<Record<string, PlotParams>>
    ),
    orderGroup: dequal(target.orderGroup, value.orderGroup) ? target.orderGroup : value.orderGroup,
    groups: value.orderGroup.reduce(
      (res, gK) => {
        res[gK] = dequal(target.groups[gK], value.groups[gK]) ? target.groups[gK] : value.groups[gK];
        return res;
      },
      {} as Partial<Record<string, GroupInfo>>
    ),
    orderVariables: dequal(target.orderVariables, value.orderVariables) ? target.orderVariables : value.orderVariables,
    variables: value.orderVariables.reduce(
      (res, gK) => {
        res[gK] = dequal(target.variables[gK], value.variables[gK]) ? target.variables[gK] : value.variables[gK];
        return res;
      },
      {} as Partial<Record<string, VariableParams>>
    ),
  };
}
