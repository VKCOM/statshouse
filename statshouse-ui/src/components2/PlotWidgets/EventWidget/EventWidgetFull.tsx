// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { PlotWidgetFullRouterProps } from '../PlotWidgetFullRouter';
import cn from 'classnames';
import css from '@/components2/Plot/style.module.css';
import { PlotControl } from '@/components2';
import { EventWidget } from '@/components2/PlotWidgets/EventWidget/EventWidget';

export function EventWidgetFull({ className, ...props }: PlotWidgetFullRouterProps) {
  return (
    <div className={className}>
      <div className={cn(css.plotColumn, 'position-relative col col-12 col-lg-7 col-xl-8')}>
        <EventWidget {...props} />
      </div>
      <div className={cn('col col-12 col-lg-5 col-xl-4')}>
        <PlotControl />
      </div>
    </div>
  );
}
