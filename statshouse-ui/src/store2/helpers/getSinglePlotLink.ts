// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { getDefaultParams, getNewMetric, type PlotKey, type QueryParams, urlEncode } from '@/url2';
import { addPlot } from './addPlot';

export function getSinglePlotLink(plotKey: PlotKey, params: QueryParams, saveParams?: QueryParams): string {
  const nextParams = addPlot(params.plots[plotKey] ?? getNewMetric(), getDefaultParams());
  return '?' + new URLSearchParams(urlEncode(nextParams, saveParams)).toString();
}
