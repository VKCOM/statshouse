// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { getNewPlot, type QueryParams, urlEncode } from 'url2';
import { deepClone } from 'common/helpers';
import { addPlot } from './addPlot';

export function getAddPlotLink(params: QueryParams, saveParams?: QueryParams): string {
  const tabNum = params.plots[params.tabNum] ? params.tabNum : params.orderPlot.slice(-1)[0];
  const plot = deepClone(params.plots[tabNum]) ?? getNewPlot();
  const nextParams = addPlot(plot, params);
  return '?' + new URLSearchParams(urlEncode(nextParams, saveParams)).toString();
}
