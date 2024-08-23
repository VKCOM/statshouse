// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import cn from 'classnames';

export type PlotHeaderTooltipContentProps = { name: React.ReactNode; description?: string };
export function PlotHeaderTooltipContent({ name, description }: PlotHeaderTooltipContentProps) {
  return (
    <div className="small text-secondary overflow-auto">
      <div className={cn('font-monospace fw-bold', description && 'mb-3')}>{name}</div>
      {!!description && <div style={{ maxWidth: '80vw', whiteSpace: 'pre-wrap' }}>{description}</div>}
      {!!description && (
        <div className="opacity-0 overflow-hidden h-0" style={{ maxWidth: '80vw', whiteSpace: 'pre' }}>
          {description}
        </div>
      )}
    </div>
  );
}
