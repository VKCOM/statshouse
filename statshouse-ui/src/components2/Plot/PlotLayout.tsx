// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo } from 'react';
import cn from 'classnames';
import { PlotWidget } from '@/components2/PlotWidgets/PlotWidget';
import { PlotWidgetFull } from '@/components2/PlotWidgets/PlotWidgetFull';
import type { PlotKey } from '@/url2';

export type PlotLayoutProps = {
  className?: string;
  plotKey: PlotKey;
  isEmbed?: boolean;
};

export const PlotLayout = memo(function PlotLayout({ className, plotKey, isEmbed }: PlotLayoutProps) {
  if (isEmbed) {
    return (
      <div className={className}>
        <PlotWidget plotKey={plotKey} isEmbed fixRatio />
      </div>
    );
  }
  return (
    <div className={cn('container-xl', className)}>
      <PlotWidgetFull plotKey={plotKey} className="row" fixRatio />
    </div>
  );
});
