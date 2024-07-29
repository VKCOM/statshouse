// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { arrToObj, type PlotParams, toTreeObj, urlDecode } from 'url2';
import { isNotNil } from 'common/helpers';

export function getPlotByUrl(url: string): PlotParams[] {
  try {
    const getUrl = new URL(url, window.document.location.origin);
    const tree = toTreeObj(arrToObj([...getUrl.searchParams.entries()]));
    const params = urlDecode(tree);
    return Object.values(params.plots).filter(isNotNil);
  } catch (e) {
    return [];
  }
}
