// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { defaultMetric, type PlotParams } from '@/url2';
import type { ProduceUpdate } from '@/store2/helpers';
import { createContext } from 'react';
import { emptyFunction } from '@/common/helpers';

export type WidgetPlotContextProps = {
  plot: PlotParams;
  setPlot: (next: ProduceUpdate<PlotParams>, replace?: boolean) => void;
  removePlot: () => void;
};

export const WidgetPlotContext = createContext<WidgetPlotContextProps>({
  plot: defaultMetric,
  setPlot: emptyFunction,
  removePlot: emptyFunction,
});
