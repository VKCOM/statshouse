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
import { useVariablesPlotByPromQL } from 'hooks/useVariablesPlotByPromQL';
import { PlotControlView } from './PlotControlView';
import { PlotControlEventOverlay } from './PlotControlEventOverlay';

export function PlotControlPromQL({ plotKey }: PlotControlProps) {
  const plotVariables = useVariablesPlotByPromQL(plotKey);

  return (
    <div className="d-flex flex-column gap-3">
      <div className="d-flex gap-2">
        <div className="input-group">
          <PlotControlAggregation plotKey={plotKey} />
          <PlotControlUnit plotKey={plotKey} />
        </div>
        <PlotControlMaxHost plotKey={plotKey} />
        <PlotControlPromQLSwitch plotKey={plotKey} />
      </div>

      <div className="d-flex flex-column gap-2">
        <div className="d-flex flex-row gap-1 w-100">
          <PlotControlFrom />
          <PlotControlView plotKey={plotKey} />
        </div>
        <PlotControlTo />
        <PlotControlGlobalTimeShifts className="w-100" />
        <PlotControlEventOverlay plotKey={plotKey} className="input-group-sm" />
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
