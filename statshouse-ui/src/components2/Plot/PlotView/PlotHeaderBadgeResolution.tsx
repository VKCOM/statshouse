import { Tooltip } from 'components/UI';
import cn from 'classnames';
import React from 'react';

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
