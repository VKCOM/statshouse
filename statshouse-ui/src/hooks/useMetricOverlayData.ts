// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useApiQueries } from '@/api/query';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { useEffect, useMemo, useState } from 'react';
import { PlotData } from '@/store2/plotDataStore';
import { PlotKey } from '@/url2';
import { normalizePlotData } from '@/store2/plotDataStore/normalizePlotData';
import { produce } from 'immer';
import { getEmptyPlotData } from '@/store2/plotDataStore/getEmptyPlotData';
import { StatsHouseStore, useStatsHouseShallow } from '@/store2';
import { isNotNil } from '@/common/helpers';

const selectorStore = ({ params: { plots, timeRange, timeShifts, variables } }: StatsHouseStore) => ({
  plots,
  timeRange,
  timeShifts,
  variables,
});

export function useMetricOverlayData() {
  const { plots, timeRange, timeShifts, variables } = useStatsHouseShallow(selectorStore);
  const {
    plot: { events },
  } = useWidgetPlotContext();

  const [data, setData] = useState<Partial<Record<PlotKey, PlotData>>>({});

  const plotEvents = useMemo(() => events.map((pK) => plots[pK]).filter(isNotNil), [events, plots]);

  const queryData = useApiQueries(plotEvents, timeRange, timeShifts, variables, undefined, true);

  useEffect(() => {
    if (queryData?.data) {
      setData((d) =>
        plotEvents.reduce(
          (res, plot) => {
            res[plot.id] = d[plot.id] ?? getEmptyPlotData();
            const data = queryData.data[plot.id]?.data;
            if (data) {
              res[plot.id] = produce(res[plot.id], normalizePlotData(data, plot, timeRange, timeShifts));
            }
            return res;
          },
          {} as Partial<Record<PlotKey, PlotData>>
        )
      );
    }
  }, [plotEvents, plots, queryData.data, timeRange, timeShifts]);
  return useMemo(() => ({ data, isLoading: queryData.isLoading }), [data, queryData.isLoading]);
}
