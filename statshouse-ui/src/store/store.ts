import { themeState, type ThemeStore } from './theme/themeStore';
import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import { statsHouseState, type StatsHouseStore } from './statshouse';
import produce from 'immer';
import { paramToVariable } from '../view/utils';
import { toKeyTag, VariableParams } from '../url/queryParams';

export type { StatsHouseStore, PlotStore, PlotValues, TopInfo } from './statshouse';
export type { ThemeStore, Theme } from './theme/themeStore';
export { THEMES } from './theme/themeStore';

export type Store = StatsHouseStore & ThemeStore;
export const useStore = create<Store, [['zustand/immer', never]]>(
  immer((...a) => ({
    ...statsHouseState(...a),
    ...themeState(...a),
  }))
);

export function setValuesVariable(nameVariable: string | undefined, values: string[]) {
  useStore.getState().setParams(
    produce((p) => {
      const i = p.variables.findIndex((v) => v.name === nameVariable);
      if (i > -1) {
        p.variables[i].values = values;
      }
    })
  );
}

export function setGroupByVariable(nameVariable: string | undefined, value: boolean) {
  useStore.getState().setParams(
    produce((p) => {
      const i = p.variables.findIndex((v) => v.name === nameVariable);
      if (i > -1) {
        p.variables[i].args.groupBy = value;
      }
    })
  );
}

export function setNegativeVariable(nameVariable: string | undefined, value: boolean) {
  useStore.getState().setParams(
    produce((p) => {
      const i = p.variables.findIndex((v) => v.name === nameVariable);
      if (i > -1) {
        p.variables[i].args.negative = value;
      }
    })
  );
}

export function setVariable(variables: VariableParams[]) {
  useStore.getState().setParams(
    produce((p) => {
      const newVariable = variables.reduce((res, { name }) => {
        res[name] = true;
        return res;
      }, {} as Record<string, boolean>);
      p.variables.forEach((variable) => {
        if (!newVariable[variable.name]) {
          variable.link.forEach(([iPlot, iTag]) => {
            if (iPlot != null && iTag != null) {
              const keyTag = toKeyTag(iTag, true);
              if (variable.args.groupBy) {
                p.plots[iPlot].groupBy = [...p.plots[iPlot].groupBy, keyTag];
              } else {
                p.plots[iPlot].groupBy = p.plots[iPlot].groupBy.filter((tag) => tag !== keyTag);
              }
              if (variable.args.negative) {
                p.plots[iPlot].filterNotIn[keyTag] = variable.values;
              } else {
                p.plots[iPlot].filterIn[keyTag] = variable.values;
              }
            }
          });
        }
      });
      const updateParam = paramToVariable({ ...p, variables: variables });
      p.variables = updateParam.variables;
      p.plots = updateParam.plots;
    })
  );
}

if (document.location.pathname === '/view' || document.location.pathname === '/embed') {
  useStore.getState().updateParamsByUrl();
}
