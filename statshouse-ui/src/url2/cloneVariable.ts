// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
