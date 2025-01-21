// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { Tooltip } from '@/components/UI';
import cn from 'classnames';

export type PlotHeaderBadgeResolutionProps = {
  resolution?: number;
  customAgg?: number;
  className?: string;
};
export function PlotHeaderBadgeResolution({ resolution, customAgg, className }: PlotHeaderBadgeResolutionProps) {
  if (resolution != null && resolution !== 1 && customAgg != null)
    return (
      <Tooltip<'span'>
        as="span"
        className={cn(
          className,
          'badge',
          resolution && customAgg > 0 && resolution > customAgg ? 'bg-danger' : 'bg-warning text-black'
        )}
        title="Custom resolution"
      >
        {resolution}s
      </Tooltip>
    );
  return null;
}
