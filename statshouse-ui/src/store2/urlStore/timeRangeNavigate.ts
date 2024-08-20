// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { getNow, PlotKey, readTimeRange } from 'url2';
import { ProduceUpdate, timeRangeAbbrevExpand } from '../helpers';
import { StatsHouseStore } from '../statsHouseStore';
import { TIME_RANGE_KEYS_TO } from 'api/enum';

export const maxTimeRange = 10 * 365 * 24 * 3600;

export function timeRangePanLeft(): ProduceUpdate<StatsHouseStore> {
  return (state) => {
    const r = state.params.timeRange;
    const minTo = getNow() - maxTimeRange - r.from;
    const delta = Math.floor(-r.from / 4);
    state.params.timeRange = readTimeRange(r.from, Math.max(r.to - delta, minTo));
  };
}

export function timeRangePanRight(): ProduceUpdate<StatsHouseStore> {
  return (state) => {
    const r = state.params.timeRange;
    if (r.absolute) {
      const delta = Math.floor(-r.from / 4);
      state.params.timeRange = readTimeRange(r.from, Math.min(r.to + delta, getNow()));
    } else {
      state.params.timeRange = readTimeRange(r.from, r.urlTo);
    }
  };
}

export function timeRangePanNow(): ProduceUpdate<StatsHouseStore> {
  return (state) => {
    const r = state.params.timeRange;
    state.params.timeRange = readTimeRange(r.from, getNow());
  };
}

export function timeRangeZoomIn(): ProduceUpdate<StatsHouseStore> {
  return (state) => {
    const r = state.params.timeRange;
    const delta = Math.floor(-r.from / 4);
    state.params.timeRange = readTimeRange(Math.floor(r.from / 2), r.to - delta);
  };
}

export function timeRangeZoomOut(): ProduceUpdate<StatsHouseStore> {
  return (state) => {
    const r = state.params.timeRange;
    const maxTo = getNow();
    const minTo = maxTo - maxTimeRange - r.from;
    const delta = Math.floor(-r.from / 2);
    state.params.timeRange = readTimeRange(Math.floor(r.from * 2), Math.max(Math.min(r.to + delta, maxTo), minTo));
  };
}

export function updateResetZoom(plotKey: PlotKey): ProduceUpdate<StatsHouseStore> {
  return (state) => {
    const plot = state.params.plots[plotKey];
    if (plot) {
      plot.yLock = { min: 0, max: 0 };
    }
    const from = timeRangeAbbrevExpand(state.baseRange);
    state.params.timeRange = readTimeRange(
      from,
      state.params.timeRange.absolute ? TIME_RANGE_KEYS_TO.default : TIME_RANGE_KEYS_TO.Now
    );
  };
}
