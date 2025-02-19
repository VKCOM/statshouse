// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { defaultMetric, PlotKey } from '@/url2';
import { type ReactNode, useCallback, useMemo } from 'react';
import { WidgetPlotContext, WidgetPlotContextProps } from '@/contexts/WidgetPlotContext';
import { WidgetPlotDataContextProvider } from '@/contexts/WidgetPlotDataContextProvider';
import { useStatsHouse } from '@/store2';
import { removePlot, setPlot } from '@/store2/methods';

export type WidgetPlotContextProviderProps = {
  children?: ReactNode;
  plotKey: PlotKey;
};

export function WidgetPlotContextProvider({ children, plotKey }: WidgetPlotContextProviderProps) {
  const plot = useStatsHouse(useCallback(({ params: { plots } }) => plots[plotKey] ?? defaultMetric, [plotKey]));
  const setPlotMemo = useMemo(() => setPlot.bind(undefined, plotKey), [plotKey]);
  const removePlotMemo = useMemo(() => removePlot.bind(undefined, plotKey), [plotKey]);

  const widgetContextValue = useMemo<WidgetPlotContextProps>(
    () => ({
      plot,
      setPlot: setPlotMemo,
      removePlot: removePlotMemo,
    }),
    [plot, removePlotMemo, setPlotMemo]
  );
  return (
    <WidgetPlotContext.Provider value={widgetContextValue}>
      <WidgetPlotDataContextProvider>{children}</WidgetPlotDataContextProvider>
    </WidgetPlotContext.Provider>
  );
}
