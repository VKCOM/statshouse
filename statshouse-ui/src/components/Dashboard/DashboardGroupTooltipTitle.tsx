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
      <div className="text-body fw-bold">
        {name}
        {!!description && ':'}
      </div>
      {!!description && <pre className="p-0 m-0">{description}</pre>}
    </div>
  );
}
