import React from 'react';

export type DashboardNameTitleProps = {
  name: string;
  description?: string;
};

export function DashboardNameTitle({ name, description }: DashboardNameTitleProps) {
  return (
    <div className="small text-secondary overflow-auto">
      <div className="text-body fw-bold">
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
