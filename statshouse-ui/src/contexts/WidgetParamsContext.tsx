// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { createContext } from 'react';
import { defaultParams, type PlotKey, type PlotParams, type QueryParams } from '@/url2';
import type { ProduceUpdate } from '@/store2/helpers';

export type WidgetParamsContextProps = {
  params: QueryParams;
  setParams: (next: ProduceUpdate<QueryParams>, replace?: boolean) => void;
  setPlot: (plotKey: PlotKey, next: ProduceUpdate<PlotParams>, replace?: boolean) => void;
  removePlot: (plotKey: PlotKey) => void;
};
export const WidgetParamsContext = createContext<WidgetParamsContextProps>({
  params: defaultParams,
  setParams: () => undefined,
  setPlot: () => undefined,
  removePlot: () => undefined,
});
