// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { StatsHouseStore } from '../statsHouseStore';
import type { PlotKey } from '@/url2';
import { PLOT_TYPE, type PlotType, QUERY_WHAT, TAG_KEY } from '@/api/enum';
import { ProduceUpdate } from '../helpers';
import { dequal } from 'dequal/lite';
import { getTimeShifts, timeShiftAbbrevExpand } from '@/view/utils2';
import { getMetricMeta } from '@/api/metric';

export function updatePlotType(plotKey: PlotKey, nextType: PlotType): ProduceUpdate<StatsHouseStore> {
  return (store) => {
    const plot = store.params.plots[plotKey];
    if (plot) {
      plot.type = nextType;
      plot.eventsHide = [];
      plot.eventsBy = [];
      switch (plot.type) {
        case PLOT_TYPE.Metric:
          plot.customAgg = 0;
          plot.what = [QUERY_WHAT.countNorm];
          plot.numSeries = 5;
          break;
        case PLOT_TYPE.Event: {
          plot.what = [QUERY_WHAT.count];
          plot.numSeries = 0;
          plot.customAgg = -1;
          const meta = getMetricMeta(plot.metricName);
          if (meta) {
            plot.eventsBy = [
              ...meta.tagsOrder.filter((tagKey) => !(meta.tagsObject[tagKey]?.description === '-')),
              ...(meta.string_top_name || meta.string_top_description ? [TAG_KEY._s] : []),
            ];
          }
          break;
        }
      }

      if (plot.type === PLOT_TYPE.Metric) {
        store.params.orderPlot.forEach((pK) => {
          const pl = store.params.plots[pK];
          if (pl?.events.indexOf(plotKey)) {
            pl.events = pl.events.filter((epK) => epK !== plotKey);
          }
        });
      }
      const timeShiftsSet = getTimeShifts(plot.customAgg);
      const shifts = store.params.timeShifts.filter(
        (v) => timeShiftsSet.find((shift) => timeShiftAbbrevExpand(shift) === v) !== undefined
      );
      if (!dequal(store.params.timeShifts, shifts)) {
        store.params.timeShifts = shifts;
      }
    }
  };
}
