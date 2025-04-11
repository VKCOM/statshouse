// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { PlotKey } from '@/url2';
import type { ReactNode } from 'react';
import { WidgetPlotContext } from '@/contexts/WidgetPlotContext';

export type WidgetPlotContextProviderProps = {
  children?: ReactNode;
  plotKey: PlotKey;
};

export function WidgetPlotContextProvider({ children, plotKey }: WidgetPlotContextProviderProps) {
  return <WidgetPlotContext.Provider value={plotKey}>{children}</WidgetPlotContext.Provider>;
}
