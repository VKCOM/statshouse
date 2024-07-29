// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { type PlotKey, type QueryParams, urlEncode } from 'url2';
import { produce } from 'immer';

export function getPlotLink(plotKey: PlotKey, params: QueryParams, saveParams?: QueryParams): string {
  return (
    '?' +
    new URLSearchParams(
      urlEncode(
        produce(params, (p) => {
          p.tabNum = plotKey;
        }),
        saveParams
      )
    ).toString()
  );
}
