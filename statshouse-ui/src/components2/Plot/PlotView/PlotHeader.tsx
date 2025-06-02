// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo } from 'react';
import { PlotHeaderNormal } from '@/components2/Plot/PlotView/PlotHeaderNormal';
import { PlotHeaderDashboard } from '@/components2/Plot/PlotView/PlotHeaderDashboard';
import { PlotHeaderEmbed } from '@/components2/Plot/PlotView/PlotHeaderEmbed';
import { PlotHeaderEmbedDashboard } from '@/components2/Plot/PlotView/PlotHeaderEmbedDashboard';

export type PlotHeaderProps = { isDashboard?: boolean; isEmbed?: boolean };

export const PlotHeader = memo(function PlotHeader({ isDashboard, isEmbed }: PlotHeaderProps) {
  if (isDashboard && isEmbed) {
    return <PlotHeaderEmbedDashboard />;
  }
  if (isDashboard) {
    return <PlotHeaderDashboard />;
  }
  if (isEmbed) {
    return <PlotHeaderEmbed />;
  }
  return <PlotHeaderNormal />;
});
