// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo } from 'react';
import { PLOT_TYPE } from '@/api/enum';
import { PlotViewMetric } from './PlotViewMetric';
import { PlotViewEvent } from './PlotViewEvent';
import { PlotKey } from '@/url2';
import { useStatsHouse } from '@/store2';

export type PlotViewProps = {
  className?: string;
  plotKey: PlotKey;
  isDashboard?: boolean;
};
export const PlotView = memo(function PlotView(props: PlotViewProps) {
  const type = useStatsHouse((s) => s.params.plots[props.plotKey]?.type);
  switch (type) {
    case PLOT_TYPE.Metric:
      return <PlotViewMetric {...props}></PlotViewMetric>;
    case PLOT_TYPE.Event:
      return <PlotViewEvent {...props}></PlotViewEvent>;
    default:
      return null;
  }
});
