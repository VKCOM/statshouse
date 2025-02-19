// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { PlotControlFilter } from './PlotControlFilter';
import { PlotControlPromQL } from './PlotControlPromQL';
import { isPromQL } from '@/store2/helpers';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';

export type PlotControlProps = {
  className?: string;
};
export function PlotControl(props: PlotControlProps) {
  const { plot } = useWidgetPlotContext();
  const isProm = isPromQL(plot);
  if (isProm) {
    return <PlotControlPromQL />;
  }
  return <PlotControlFilter {...props}></PlotControlFilter>;
}
