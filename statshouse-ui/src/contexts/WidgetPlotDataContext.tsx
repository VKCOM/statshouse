// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { ProduceUpdate } from '@/store2/helpers';
import { createContext } from 'react';
import { emptyFunction } from '@/common/helpers';
import { PlotData } from '@/store2/plotDataStore';
import { getEmptyPlotData } from '@/store2/plotDataStore/getEmptyPlotData';

export type WidgetPlotDataContextProps = {
  plotData: PlotData;
  setPlotData: (next: ProduceUpdate<PlotData>) => void;
};

export const WidgetPlotDataContext = createContext<WidgetPlotDataContextProps>({
  plotData: getEmptyPlotData(),
  setPlotData: emptyFunction,
});
