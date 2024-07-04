// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import { type PlotControlProps } from './PlotControl';
import { PlotControlFrom } from './PlotControlFrom';
import { PlotControlTo } from './PlotControlTo';
import { PlotControlGlobalTimeShifts } from './PlotControlGlobalTimeShifts';
import { PlotControlAggregation } from './PlotControlAggregation';
import { PlotControlPromQLSwitch } from './PlotControlPromQLSwitch';
import { PlotControlMaxHost } from './PlotControlMaxHost';
import { PlotControlUnit } from './PlotControlUnit';
import { PlotControlPromQLEditor } from './PlotControlPromQLEditor';
import { PlotControlFilterVariable } from './PlotControlFilterVariable';
import { useStatsHouse } from 'store2';
import { ErrorMessages } from 'components';

function findVariable(name?: string, promQL?: string) {
  return !!name && !!promQL && promQL.indexOf(name) > -1;
}
function filterVariableByPromQl<T extends { name: string }>(promQL?: string): (v?: T) => v is NonNullable<T> {
  return (v): v is NonNullable<T> => findVariable(v?.name, promQL);
}

export function PlotControlPromQL({ plotKey }: PlotControlProps) {
  const plotVariables = useStatsHouse((s) =>
    Object.values(s.params.variables).filter(filterVariableByPromQl(s.params.plots[plotKey]?.promQL))
  );

  return (
    <div className="d-flex flex-column gap-3">
      <ErrorMessages />
      <div className="d-flex gap-2">
        <div className="input-group">
          <PlotControlAggregation plotKey={plotKey} />
          <PlotControlUnit plotKey={plotKey} />
        </div>
        <PlotControlMaxHost plotKey={plotKey} />
        <PlotControlPromQLSwitch plotKey={plotKey} />
      </div>

      <div className="d-flex flex-column gap-2">
        <PlotControlFrom />
        <div className="align-items-baseline w-100">
          <PlotControlTo />
        </div>
        <PlotControlGlobalTimeShifts className="w-100" />
      </div>
      <div className="d-flex flex-column gap-2">
        {plotVariables.map((variable) => (
          <PlotControlFilterVariable key={variable.id} variableKey={variable.id} />
        ))}
      </div>
      <PlotControlPromQLEditor plotKey={plotKey} className="" />
    </div>
  );
}
