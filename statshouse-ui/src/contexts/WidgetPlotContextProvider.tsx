// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { defaultMetric, PlotKey } from '@/url2';
import { type ReactNode, useMemo } from 'react';
import { WidgetPlotContext, WidgetPlotContextProps } from '@/contexts/WidgetPlotContext';
import { useWidgetParamsContext } from '@/contexts/useWidgetParamsContext';
import { WidgetPlotDataContextProvider } from '@/contexts/WidgetPlotDataContextProvider';

export type WidgetPlotContextProviderProps = {
  children?: ReactNode;
  plotKey: PlotKey;
};

export function WidgetPlotContextProvider({ children, plotKey }: WidgetPlotContextProviderProps) {
  const { params, setPlot, removePlot } = useWidgetParamsContext();
  const plot = params.plots[plotKey] ?? defaultMetric;
  const setPlotMemo = useMemo(() => setPlot.bind(undefined, plotKey), [plotKey, setPlot]);
  const removePlotMemo = useMemo(() => removePlot.bind(undefined, plotKey), [plotKey, removePlot]);

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
