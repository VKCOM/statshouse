// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { PlotWidgetFullRouterProps } from '../PlotWidgetFullRouter';
import cn from 'classnames';
import css from '@/components2/Plot/style.module.css';
import { PlotControl } from '@/components2';
import { isPromQL } from '@/store2/helpers';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { MetricWidget } from './MetricWidget';
import { usePlotsDataStore } from '@/store2/plotDataStore';
import { useCallback } from 'react';

export function MetricWidgetFull({ className, ...props }: PlotWidgetFullRouterProps) {
  const { plot } = useWidgetPlotContext();

  const isProm = isPromQL(plot);
  const promqlExpand = usePlotsDataStore(useCallback(({ plotsData }) => !!plotsData[plot.id]?.promqlExpand, [plot.id]));
  const plotDataPromqlExpand = isProm && promqlExpand;
  return (
    <div className={className}>
      <div
        className={cn(
          css.plotColumn,
          'position-relative col col-12',
          plotDataPromqlExpand ? 'col-lg-5 col-xl-4' : 'col-lg-7 col-xl-8'
        )}
      >
        <MetricWidget {...props} />
      </div>
      <div className={cn('col col-12', plotDataPromqlExpand ? 'col-lg-7 col-xl-8' : 'col-lg-5 col-xl-4')}>
        <PlotControl />
      </div>
    </div>
  );
}
