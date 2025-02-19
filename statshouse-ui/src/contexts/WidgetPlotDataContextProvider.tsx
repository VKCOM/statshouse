import { ReactNode, useCallback, useEffect, useMemo, useState } from 'react';
import { useWidgetParamsContext } from '@/contexts/useWidgetParamsContext';
import { PlotData } from '@/store2/plotDataStore';
import { getEmptyPlotData } from '@/store2/plotDataStore/getEmptyPlotData';
import type { ProduceUpdate } from '@/store2/helpers';
import { produce } from 'immer';
import { WidgetPlotDataContext, WidgetPlotDataContextProps } from '@/contexts/WidgetPlotDataContext';
import { updateTitle } from '@/store2/helpers/updateTitle';

export type WidgetPlotDataContextProviderProps = {
  children?: ReactNode;
};

export function WidgetPlotDataContextProvider({ children }: WidgetPlotDataContextProviderProps) {
  const { params } = useWidgetParamsContext();
  const [plotData, setPlotData] = useState<PlotData>(getEmptyPlotData());

  const setPlotDataProduce = useCallback<(draft: ProduceUpdate<PlotData>) => void>(
    (draft) => setPlotData(produce(draft)),
    []
  );

  const widgetContextValue = useMemo<WidgetPlotDataContextProps>(
    () => ({
      plotData,
      setPlotData: setPlotDataProduce,
    }),
    [plotData, setPlotDataProduce]
  );

  useEffect(() => {
    updateTitle(params, plotData);
  }, [params, plotData]);

  return <WidgetPlotDataContext.Provider value={widgetContextValue}>{children}</WidgetPlotDataContext.Provider>;
}
