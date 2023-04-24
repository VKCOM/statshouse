// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo } from 'react';
import { PlotViewMetric, PlotViewEvent } from '../index';

import { PLOT_TYPE, PlotType } from '../../common/plotQueryParams';

type PlotViewProps = {
  indexPlot: number;
  type: PlotType;
  className?: string;
  dashboard?: boolean;
  compact: boolean;
  yAxisSize: number;
  group?: string;
};

function _PlotView({ type, ...props }: PlotViewProps) {
  switch (type) {
    case PLOT_TYPE.Event:
      return <PlotViewEvent {...props} />;
    case PLOT_TYPE.Metric:
    default:
      return <PlotViewMetric {...props} />;
  }
}

export const PlotView = memo(_PlotView);
