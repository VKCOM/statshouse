import React from 'react';
import cn from 'classnames';

export type PlotHeaderTooltipContentProps = { name: React.ReactNode; description?: string };
export function PlotHeaderTooltipContent({ name, description }: PlotHeaderTooltipContentProps) {
  return (
    <div className="small text-secondary overflow-auto">
      <div className={cn('font-monospace fw-bold', description && 'mb-3')}>{name}</div>
      {!!description && <pre className="p-0 m-0">{description}</pre>}
    </div>
  );
}
