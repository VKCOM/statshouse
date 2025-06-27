// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { getDefaultParams, metricEncode, orderPlotSplitter, QueryParams, removeValueChar } from '@/url2';
import { GET_PARAMS, PLOT_TYPE } from '@/api/enum';
import { dequal } from 'dequal/lite';
import { getFullDashSave } from '@/common/migrate/migrate3to4';

export function widgetsParamsEncode(
  params: QueryParams,
  defaultParams: QueryParams = getDefaultParams()
): [string, string][] {
  const paramArr: [string, string][] = [];
  if (defaultParams.plots !== params.plots) {
    Object.values(params.plots).forEach((p) => {
      if (p) {
        switch (p.type) {
          case PLOT_TYPE.Metric:
          case PLOT_TYPE.Event:
            paramArr.push(...metricEncode(p, defaultParams.plots[p.id]));
            break;
          default:
        }
      }
    });
    //remove plots
    Object.values(defaultParams.plots).forEach((dPlot) => {
      if (dPlot && !params.plots[dPlot.id]) {
        paramArr.push([GET_PARAMS.plotPrefix + dPlot.id, removeValueChar]);
      }
    });
  }
  if (getFullDashSave()) {
    if (params.orderPlot != null && !dequal(defaultParams.orderPlot, params.orderPlot)) {
      paramArr.push([GET_PARAMS.orderPlot, params.orderPlot.join(orderPlotSplitter)]);
    }
  }
  return paramArr;
}
