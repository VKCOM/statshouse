import React from 'react';
export type DashboardGroupTooltipTitleProps = {
  name?: string;
  description?: string;
};
export function DashboardGroupTooltipTitle({ name, description }: DashboardGroupTooltipTitleProps) {
  if (!name) {
    return null;
  }
  return (
    <div className="small text-secondary overflow-auto">
      <div className="font-monospace text-body fw-bold">
        {name}
        {!!description && ':'}
      </div>
      {!!description && <div style={{ maxWidth: '80vw', whiteSpace: 'pre-wrap' }}>{description}</div>}
      {!!description && (
        <div className="opacity-0 overflow-hidden h-0" style={{ maxWidth: '80vw', whiteSpace: 'pre' }}>
          {description}
        </div>
      )}
    </div>
  );
}
