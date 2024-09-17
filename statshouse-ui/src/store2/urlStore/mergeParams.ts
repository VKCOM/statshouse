import { type GroupInfo, type PlotParams, type QueryParams, type VariableParams } from 'url2';
import { dequal } from 'dequal/lite';

export function mergeParams(target: QueryParams, value: QueryParams): QueryParams {
  if (target === value) {
    return target;
  }
  let changePlots = false;
  const nextPlots = value.orderPlot.reduce(
    (res, pK) => {
      if (dequal(target.plots[pK], value.plots[pK])) {
        res[pK] = target.plots[pK];
      } else {
        changePlots = true;
      }
      res[pK] = dequal(target.plots[pK], value.plots[pK]) ? target.plots[pK] : value.plots[pK];
      return res;
    },
    { ...value.plots }
  );
  let changeGroups = false;
  const nextGroups = value.orderGroup.reduce(
    (res, gK) => {
      if (dequal(target.groups[gK], value.groups[gK])) {
        res[gK] = target.groups[gK];
      } else {
        changeGroups = true;
      }
      return res;
    },
    { ...value.groups }
  );
  let changeVariables = false;
  const nextVariables = value.orderVariables.reduce(
    (res, gK) => {
      if (dequal(target.variables[gK], value.variables[gK])) {
        res[gK] = target.variables[gK];
      } else {
        changeVariables = true;
      }
      return res;
    },
    { ...value.variables }
  );

  return {
    ...value,
    orderPlot: dequal(target.orderPlot, value.orderPlot) ? target.orderPlot : value.orderPlot,
    plots: changePlots ? nextPlots : target.plots,
    orderGroup: dequal(target.orderGroup, value.orderGroup) ? target.orderGroup : value.orderGroup,
    groups: changeGroups ? nextGroups : target.groups,
    orderVariables: dequal(target.orderVariables, value.orderVariables) ? target.orderVariables : value.orderVariables,
    variables: changeVariables ? nextVariables : target.variables,
  };
}
