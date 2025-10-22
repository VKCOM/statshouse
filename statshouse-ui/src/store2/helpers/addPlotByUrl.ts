// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { QueryParams } from '@/url2';

import { getPlotByUrl } from './getPlotByUrl';
import { addPlots } from '@/store2/helpers';

export function addPlotByUrl(url: string, params: QueryParams) {
  return addPlots(getPlotByUrl(url), params);
}
