// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useCallback, useContext, useMemo } from 'react';
import { WidgetPlotContext } from '@/contexts/WidgetPlotContext';
import { defaultMetric } from '@/url2';
import { useStatsHouse } from '@/store2';
import { removePlot, setPlot } from '@/store2/methods';

export function useWidgetPlotContext() {
  const plotKey = useContext(WidgetPlotContext);
  const setPlotMemo = useMemo(() => setPlot.bind(undefined, plotKey), [plotKey]);
  const removePlotMemo = useMemo(() => removePlot.bind(undefined, plotKey), [plotKey]);
  const plot = useStatsHouse(useCallback(({ params: { plots } }) => plots[plotKey] ?? defaultMetric, [plotKey]));
  return useMemo(
    () => ({ plot, setPlot: setPlotMemo, removePlot: removePlotMemo }),
    [plot, removePlotMemo, setPlotMemo]
  );
}
