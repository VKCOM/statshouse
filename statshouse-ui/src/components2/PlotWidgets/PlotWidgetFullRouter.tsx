// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { PLOT_TYPE } from '@/api/enum';
import { MetricWidgetFull } from '@/components2/PlotWidgets/MetricWidget/MetricWidgetFull';
import { EventWidgetFull } from '@/components2/PlotWidgets/EventWidget/EventWidgetFull';
import type { PlotWidgetRouterProps } from '@/components2/PlotWidgets/PlotWidgetRouter';

export type PlotWidgetFullRouterProps = {
  className?: string;
  isDashboard?: boolean;
  isEmbed?: boolean;
  fixRatio?: boolean;
};

export function PlotWidgetFullRouter(props: PlotWidgetRouterProps) {
  const {
    plot: { type },
  } = useWidgetPlotContext();
  switch (type) {
    case PLOT_TYPE.Metric:
      return <MetricWidgetFull {...props} />;
    case PLOT_TYPE.Event:
      return <EventWidgetFull {...props} />;
    default:
      return null;
  }
}
