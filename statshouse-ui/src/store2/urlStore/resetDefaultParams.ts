// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { getHomePlot, QueryParams, readTimeRange } from '@/url2';
import { produce } from 'immer';
import { globalSettings } from '@/common/settings';
import { METRIC_VALUE_BACKEND_VERSION, TIME_RANGE_KEYS_TO } from '@/api/enum';
import { addPlot, timeRangeAbbrevExpand } from '../helpers';
import { defaultBaseRange } from '@/store2';

export function resetDefaultParams(params: QueryParams) {
  let reset = false;
  if (Object.keys(params.plots).length === 0) {
    params = addPlot(getHomePlot(), params);
    reset = true;
  }
  params = produce(params, (p) => {
    if (+p.tabNum >= 0 && !p.plots[params.tabNum]) {
      p.tabNum = Object.keys(p.plots).length > 1 ? '-1' : '0';
      reset = true;
    }

    if (globalSettings.disabled_v1) {
      Object.values(p.plots).forEach((plot) => {
        if (plot && plot.backendVersion === METRIC_VALUE_BACKEND_VERSION.v1) {
          plot.backendVersion = METRIC_VALUE_BACKEND_VERSION.v2;
          reset = true;
        }
      });
    }
    if (!p.timeRange.from && p.timeRange.urlTo === TIME_RANGE_KEYS_TO.default) {
      const from = timeRangeAbbrevExpand(defaultBaseRange);
      p.timeRange = readTimeRange(from, TIME_RANGE_KEYS_TO.default);
      reset = true;
    } else if (p.timeRange.urlTo === TIME_RANGE_KEYS_TO.default) {
      p.timeRange.urlTo = p.timeRange.to = p.timeRange.now;
      reset = true;
    } else if (p.timeRange.from >= 0) {
      p.timeRange.from = timeRangeAbbrevExpand(defaultBaseRange);
      reset = true;
    }
  });
  if (reset && params.dashboardId == null) {
    return params;
  }
}
