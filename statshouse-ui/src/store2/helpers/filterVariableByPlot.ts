// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { filterVariableByLink } from './filterVariableByLink';
import { type PlotParams, type VariableParamsLink } from 'url2';
import { isPromQL } from './isPromQL';
import { filterVariableByPromQl } from './filterVariableByPromQl';

export function filterVariableByPlot<T extends { name: string; link: VariableParamsLink[] }>(
  plot?: PlotParams
): (v?: T) => v is NonNullable<T> {
  return isPromQL(plot) ? filterVariableByPromQl<T>(plot?.promQL) : filterVariableByLink<T>(plot?.id);
}
