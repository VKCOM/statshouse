// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { PLOT_TYPE } from '@/api/enum';
import { MetricWidget } from '@/components2/PlotWidgets/MetricWidget/MetricWidget';
import { EventWidget } from '@/components2/PlotWidgets/EventWidget/EventWidget';

export type PlotWidgetRouterProps = {
  className?: string;
  isDashboard?: boolean;
  isEmbed?: boolean;
  fixRatio?: boolean;
};

export function PlotWidgetRouter(props: PlotWidgetRouterProps) {
  const {
    plot: { type },
  } = useWidgetPlotContext();
  switch (type) {
    case PLOT_TYPE.Metric:
      return <MetricWidget {...props} />;
    case PLOT_TYPE.Event:
      return <EventWidget {...props} />;
    default:
      return null;
  }
}
