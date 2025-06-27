// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { QueryParams } from '@/url2/queryParams';

export function urlDecodeVersion(params: Pick<QueryParams, 'plots' | 'orderPlot'>) {
  if (Object.values(params.plots).some((plot) => plot && plot.group != null && plot.layout)) {
    return '4';
  }
  return '3';
}
