import { useStatsHouseShallow } from 'store2';
import { useMemo } from 'react';
import type { PlotKey } from 'url2';
import { PlotVariablesLink } from 'store2/plotsInfoStore';
import { TagKey } from '../api/enum';

export function useVariableLink(plotKey: PlotKey, tagKey: TagKey) {
  // todo: optimize multi use;
  const { variables } = useStatsHouseShallow(({ params: { variables } }) => ({ variables }));
  return useMemo(() => {
    const plotVariablesLink: Partial<Record<PlotKey, PlotVariablesLink>> = {};
    Object.values(variables).forEach((variable) => {
      variable?.link.forEach(([plotKey, tagKey]) => {
        const p = (plotVariablesLink[plotKey] ??= {});
        p[tagKey] = { variableKey: variable.id, variableName: variable.name };
      });
    });
    return plotVariablesLink[plotKey]?.[tagKey];
  }, [plotKey, tagKey, variables]);
}
