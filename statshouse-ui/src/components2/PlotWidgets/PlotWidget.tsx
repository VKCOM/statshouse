// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { WidgetPlotContextProvider } from '@/contexts/WidgetPlotContextProvider';
import type { PlotKey } from '@/url2';
import { PlotWidgetRouter, type PlotWidgetRouterProps } from '@/components2/PlotWidgets/PlotWidgetRouter';
import { memo } from 'react';

export type PlotWidgetProps = {
  plotKey: PlotKey;
} & PlotWidgetRouterProps;

export const PlotWidget = memo(function PlotWidget({ plotKey, ...props }: PlotWidgetProps) {
  return (
    <WidgetPlotContextProvider plotKey={plotKey}>
      <PlotWidgetRouter {...props} />
    </WidgetPlotContextProvider>
  );
});
