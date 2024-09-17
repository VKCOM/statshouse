// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo } from 'react';
import { PLOT_TYPE } from 'api/enum';
import { PlotViewMetric } from './PlotViewMetric';
import { PlotViewEvent } from './PlotViewEvent';
import { PlotKey } from 'url2';
import { useStatsHouse } from 'store2';

export type PlotViewProps = {
  className?: string;
  plotKey: PlotKey;
  isDashboard?: boolean;
};
export function _PlotView(props: PlotViewProps) {
  const type = useStatsHouse((s) => s.params.plots[props.plotKey]?.type);
  switch (type) {
    case PLOT_TYPE.Metric:
      return <PlotViewMetric {...props}></PlotViewMetric>;
    case PLOT_TYPE.Event:
      return <PlotViewEvent {...props}></PlotViewEvent>;
    default:
      return null;
  }
}

export const PlotView = memo(_PlotView);
