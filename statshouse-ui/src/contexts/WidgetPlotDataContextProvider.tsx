import { ReactNode, useCallback, useEffect, useMemo } from 'react';
import { clearPlotData, setPlotData, usePlotsDataStore } from '@/store2/plotDataStore';
import { emptyPlotData } from '@/store2/plotDataStore/getEmptyPlotData';
import { WidgetPlotDataContext, WidgetPlotDataContextProps } from '@/contexts/WidgetPlotDataContext';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { useStatsHouse } from '@/store2';

export type WidgetPlotDataContextProviderProps = {
  children?: ReactNode;
};

export function WidgetPlotDataContextProvider({ children }: WidgetPlotDataContextProviderProps) {
  const { plot } = useWidgetPlotContext();
  const plotData = usePlotsDataStore(useCallback(({ plotsData }) => plotsData[plot.id] ?? emptyPlotData, [plot.id]));
  const setPlotDataProduce = useMemo(() => setPlotData.bind(undefined, plot.id), [plot.id]);
  useEffect(
    () => () => {
      if (!useStatsHouse.getState().params.plots[plot.id]) {
        clearPlotData(plot.id);
      }
    },
    [plot.id]
  );
  const widgetContextValue = useMemo<WidgetPlotDataContextProps>(
    () => ({
      plotData,
      setPlotData: setPlotDataProduce,
    }),
    [plotData, setPlotDataProduce]
  );

  return <WidgetPlotDataContext.Provider value={widgetContextValue}>{children}</WidgetPlotDataContext.Provider>;
}
