// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { QueryParams } from 'url2';
import { addPlot } from './addPlot';

import { getPlotByUrl } from './getPlotByUrl';

export function addPlotByUrl(url: string, params: QueryParams) {
  let nextParams = params;
  getPlotByUrl(url).forEach((plot) => {
    nextParams = addPlot(plot, nextParams);
  });
  return nextParams;
}
