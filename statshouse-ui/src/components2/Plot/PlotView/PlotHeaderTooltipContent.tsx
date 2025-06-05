// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import cn from 'classnames';
import { TooltipMarkdown } from '@/components/Markdown/TooltipMarkdown';

export type PlotHeaderTooltipContentProps = {
  name: React.ReactNode;
  description?: string;
};

export function PlotHeaderTooltipContent({ name, description }: PlotHeaderTooltipContentProps) {
  const hasDescription = !!description;

  return (
    <div className="small text-secondary overflow-auto">
      <div className={cn('font-monospace fw-bold', hasDescription && 'mb-3')}>{name}</div>
      {hasDescription && <TooltipMarkdown description={description} />}
    </div>
  );
}
