import type { VariableParams, VariableParamsLink } from './queryParams';
import { deepClone } from '../common/helpers';

export function cloneVariable(variable: VariableParams): VariableParams;
export function cloneVariable(variable: undefined): undefined;
export function cloneVariable(variable?: VariableParams): VariableParams | undefined {
  if (variable == null) {
    return variable;
  }
  return {
    id: variable.id,
    name: variable.name,
    description: variable.description,
    link: [...variable.link.map(([p, t]) => [p, t] as VariableParamsLink)],
    source: deepClone(variable.source),
    sourceOrder: [...variable.sourceOrder],
    values: [...variable.values],
    negative: variable.negative,
    groupBy: variable.groupBy,
  };
}
