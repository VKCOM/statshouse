// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import {PlotWidgetRouterProps} from '../PlotWidgetRouter';
import {useWidgetPlotContext} from "@/contexts";
import {PlotViewEvent} from "@/components2/Plot/PlotView/PlotViewEvent";

export function EventWidget({ className, isDashboard }: PlotWidgetRouterProps) {
  const {
    plot: { id },
  } = useWidgetPlotContext();
  return <PlotViewEvent plotKey={id} isDashboard={isDashboard} className={className} />;
}
