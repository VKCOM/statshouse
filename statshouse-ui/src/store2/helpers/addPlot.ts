// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { getNewMetricLayout, GroupKey, PlotParams, QueryParams } from '@/url2';
import { produce } from 'immer';
import { getNextPlotKey } from '../urlStore/updateParamsPlotStruct';
import { findGroupPositionLayout } from '@/store2/urlStore';

export function addPlot(
  plot: PlotParams,
  params: QueryParams,
  groupKey?: GroupKey,
  activeInsert: boolean = true
): QueryParams {
  // todo: copy variable
  const nextId = getNextPlotKey(params);
  groupKey ??= plot.group ?? params.orderGroup.slice(-1)[0] ?? '0';
  const size = params.groups[groupKey]?.size ?? '2';
  const nextPlot = {
    ...plot,
    id: nextId,
    group: groupKey,
    layout: findGroupPositionLayout(params, plot.layout ?? getNewMetricLayout(plot.type, size), groupKey),
  };
  return produce<QueryParams>(params, (p) => {
    p.plots[nextId] = nextPlot;
    if (activeInsert) {
      p.tabNum = nextId;
    }
  });
}
