// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useEffect, useMemo } from 'react';
import { useWidgetParamsContext, useWidgetPlotContext } from '@/contexts';
import { PlotData } from '@/store2/plotDataStore';
import { normalizePlotData } from '@/store2/plotDataStore/normalizePlotData';
import { queryStart } from '@/store2/plotQueryStore';
import { emptyFunction } from '@/common/helpers';
import { ProduceUpdate } from '@/store2/helpers';
import { useApiQuery } from '@/api/query';
import { produce } from 'immer';
import { useStatsHouse } from '@/store2';

// const { setPlotHeal } = useStatsHouse.getState();

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
        });
      }
    } else if (response?.data) {
      const healsStatus = useStatsHouse.getState().plotHeals[plot.id]?.status;
      setPlotData((d) => {
        const next = produce(d, normalizePlotData(response.data, plot, params));
        next.lastHeals = true;
        if (healsStatus) {
          next.error = '';
        }
        return next;
      });
    }
  }, [error, params, plot, response, response?.data, setPlotData]);

  return useMemo(() => [plotData, setPlotData], [plotData, setPlotData]);
}
