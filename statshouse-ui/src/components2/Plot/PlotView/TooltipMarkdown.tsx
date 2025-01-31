// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import markdownStyles from '../../style.module.css';
import { MarkdownRender } from './MarkdownRender';
import cn from 'classnames';

export type ITooltipMarkdownProps = {
  description?: string;
};

const _TooltipMarkdown = ({ description }: ITooltipMarkdownProps) => (
  <>
    <div style={{ maxWidth: '80vw', maxHeight: '80vh' }}>
      <MarkdownRender className={cn(markdownStyles.markdownMargin, markdownStyles.markdownSpace)}>
        {description}
      </MarkdownRender>
    </div>
    <div className="opacity-0 overflow-hidden h-0" style={{ maxWidth: '80vw', whiteSpace: 'pre' }}>
      <MarkdownRender className={markdownStyles.markdownMargin}>{description}</MarkdownRender>
    </div>
  </>
);

export const TooltipMarkdown = React.memo(_TooltipMarkdown);
