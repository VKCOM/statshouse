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
    <div className="small">
      <div className="text-body fw-bold">
        {name}
        {!!description && ':'}
      </div>
      {!!description && <div className="text-secondary">{description}</div>}
    </div>
  );
}
