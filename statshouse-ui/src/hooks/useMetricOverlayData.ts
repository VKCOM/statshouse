import { useApiQueries } from '@/api/query';
import { useWidgetParamsContext, useWidgetPlotContext } from '@/contexts';
import { useEffect, useMemo, useState } from 'react';
import { PlotData } from '@/store2/plotDataStore';
import { PlotKey } from '@/url2';
import { normalizePlotData } from '@/store2/plotDataStore/normalizePlotData';
import { produce } from 'immer';
import { getEmptyPlotData } from '@/store2/plotDataStore/getEmptyPlotData';

export function useMetricOverlayData() {
  const { params } = useWidgetParamsContext();
  const {
    plot: { events: plotEvents },
  } = useWidgetPlotContext();

  const [data, setData] = useState<Partial<Record<PlotKey, PlotData>>>({});

  const queryData = useApiQueries(plotEvents, params);

  useEffect(() => {
    if (queryData?.data) {
      setData((d) =>
        plotEvents.reduce(
          (res, pK) => {
            res[pK] = d[pK] ?? getEmptyPlotData();
            if (queryData.data[pK]?.data && params.plots[pK]) {
              res[pK] = produce(res[pK], normalizePlotData(queryData.data[pK]?.data, params.plots[pK], params));
            }
            return res;
          },
          {} as Partial<Record<PlotKey, PlotData>>
        )
      );
    }
  }, [params, plotEvents, queryData.data]);
  return useMemo(() => ({ data, isLoading: queryData.isLoading }), [data, queryData.isLoading]);
}
