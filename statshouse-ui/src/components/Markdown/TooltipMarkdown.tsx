// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo } from 'react';
import { MarkdownRender } from './MarkdownRender';

export type ITooltipMarkdownProps = {
  description?: string;
  value?: string;
};

export const TooltipMarkdown = memo(function TooltipMarkdown({ description, value }: ITooltipMarkdownProps) {
  return (
    <>
      <div style={{ maxWidth: '80vw', maxHeight: '80vh' }}>
        <MarkdownRender value={value}>{description}</MarkdownRender>
      </div>
      <div className="opacity-0 overflow-hidden h-0" style={{ maxWidth: '80vw', whiteSpace: 'pre' }}>
        <MarkdownRender value={value}>{description}</MarkdownRender>
      </div>
    </>
  );
});
