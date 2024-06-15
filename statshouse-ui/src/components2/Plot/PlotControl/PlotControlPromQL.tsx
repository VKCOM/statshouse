// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import { type PlotControlProps } from './PlotControl';
import { getNewPlot, useUrlStore } from 'store2';
import { isNotNil } from '../../../common/helpers';
import { PlotControlFrom } from './PlotControlFrom';
import { PlotControlTo } from './PlotControlTo';
import { PlotControlGlobalTimeShifts } from './PlotControlGlobalTimeShifts';
import { PlotControlAggregation } from './PlotControlAggregation';
import { PlotControlPromQLSwitch } from './PlotControlPromQLSwitch';
import { PlotControlMaxHost } from './PlotControlMaxHost';
import { PlotControlUnit } from './PlotControlUnit';
import { PlotControlPromQLEditor } from './PlotControlPromQLEditor';
import { PlotControlFilterVariable } from './PlotControlFilterVariable';

const emptyPlot = getNewPlot();

export function PlotControlPromQL({ plot = emptyPlot }: PlotControlProps) {
  const plotKey = plot.id;
  const plotVariables = useUrlStore((s) =>
    Object.values(s.params.variables)
      .filter(isNotNil)
      .filter((v) => plot.promQL.indexOf(v.name) > -1)
  );

  return (
    <div>
      <div>
        <div className="row mb-3">
          <div className="col-12 d-flex">
            <div className="input-group  me-2">
              <PlotControlAggregation plotKey={plotKey} />
              <PlotControlUnit plotKey={plotKey} />
            </div>
            <PlotControlMaxHost plotKey={plotKey} />
            <PlotControlPromQLSwitch className="ms-3" plotKey={plotKey} />
          </div>
        </div>
        <div className="row mb-3 align-items-baseline">
          <PlotControlFrom />
          <div className="align-items-baseline mt-2">
            <PlotControlTo />
          </div>
          <PlotControlGlobalTimeShifts className="w-100 mt-2" />
        </div>
        <div>
          {plotVariables.map((variable) => (
            <PlotControlFilterVariable key={variable.id} variableKey={variable.id} />
          ))}
        </div>
        <PlotControlPromQLEditor plotKey={plotKey} className={'mb-3'} />
      </div>
    </div>
  );
}
