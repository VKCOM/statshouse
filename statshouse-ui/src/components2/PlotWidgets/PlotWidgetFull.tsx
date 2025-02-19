// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { PlotKey } from '@/url2';
import { WidgetPlotContextProvider } from '@/contexts/WidgetPlotContextProvider';
import { PlotWidgetFullRouter, type PlotWidgetFullRouterProps } from '@/components2/PlotWidgets/PlotWidgetFullRouter';
import { memo } from 'react';

export type PlotWidgetFullProps = {
  plotKey: PlotKey;
} & PlotWidgetFullRouterProps;

export const PlotWidgetFull = memo(function PlotWidgetFull({ plotKey, ...props }: PlotWidgetFullProps) {
  return (
    <WidgetPlotContextProvider plotKey={plotKey}>
      <PlotWidgetFullRouter {...props} />
    </WidgetPlotContextProvider>
  );
});
