// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { type PlotKey, type QueryParams, urlEncode } from 'url2';
import { produce } from 'immer';

let localParams: QueryParams;
let localSingleParams: QueryParams;
let localSaveParams: QueryParams;
let templateFn: (plotKey: PlotKey) => string;
let templateSaveFn: (plotKey: PlotKey) => string;

const plotKeyPh = '#$$$[pk]$$$#';

function createTemplateFn(params: QueryParams, saveParams?: QueryParams) {
  const link =
    '?' +
    new URLSearchParams(
      urlEncode(
        produce(params, (p) => {
          p.tabNum = plotKeyPh;
        }),
        saveParams
      )
    ).toString();
  const [f, p] = link.split(encodeURIComponent(plotKeyPh)).map(String);
  return (plotKey: PlotKey) => `${f}${plotKey}${p}`;
}

export function getPlotLink(plotKey: PlotKey, params: QueryParams, saveParams: QueryParams): string {
  if (localParams === params && saveParams === localSaveParams && !!templateSaveFn) {
    return templateSaveFn(plotKey);
  } else {
    localParams = params;
    localSaveParams = saveParams;
    return (templateSaveFn = createTemplateFn(params, saveParams))(plotKey);
  }
}

export function getPlotSingleLink(plotKey: PlotKey, params: QueryParams): string {
  if (localSingleParams === params && !!templateFn) {
    return templateFn(plotKey);
  } else {
    localSingleParams = params;
    return (templateFn = createTemplateFn(params))(plotKey);
  }
}
