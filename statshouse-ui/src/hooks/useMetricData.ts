// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useEffect, useMemo } from 'react';
import { useApiQuery } from '@/api/query';
import { useWidgetParamsContext, useWidgetPlotContext } from '@/contexts';
import { PlotData } from '@/store2/plotDataStore';
import { normalizePlotData } from '@/store2/plotDataStore/normalizePlotData';
import { queryStart } from '@/store2/plotQueryStore';
import { emptyFunction } from '@/common/helpers';
import { ProduceUpdate } from '@/store2/helpers';
import { useStatsHouse } from '@/store2';

// const dataSelector = (res?: ApiQuery) => ({ time: res?.data?.series.time, series_data: res?.data?.series.series_data });

export function useMetricData(visible: boolean = true): [PlotData, (next: ProduceUpdate<PlotData>) => void] {
  const { plot, plotData, setPlotData } = useWidgetPlotContext();
  const { params } = useWidgetParamsContext();
  const queryData = useApiQuery(plot, params, undefined, visible);
  const response = queryData.data;
  const error = queryData.error;
  const isLoading = queryData.isLoading;

  useEffect(() => {
    let prepareEnd: () => void = emptyFunction;
    if (isLoading) {
      prepareEnd = queryStart(plot.id);
    }
    return prepareEnd;
  });

  useEffect(() => {
    if (error) {
      if (error.status === 403) {
        setPlotData((d) => {
          d.error403 = error.toString();
        });
      } else {
        setPlotData((d) => {
          d.error = error.toString();
          d.lastHeals = false;
          // todo: add heals check
          useStatsHouse.getState().setPlotHeal(plot.id, false);
          // state.setPlotHeal(plotKey, false);
        });
      }
    }
    if (response?.data) {
      setPlotData(normalizePlotData(response.data, plot, params));
      useStatsHouse.getState().setPlotHeal(plot.id, true);
    }
  }, [error, params, plot, response?.data, setPlotData]);

  return useMemo(() => [plotData, setPlotData], [plotData, setPlotData]);
}
