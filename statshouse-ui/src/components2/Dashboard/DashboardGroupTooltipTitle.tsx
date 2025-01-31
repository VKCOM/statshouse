// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
