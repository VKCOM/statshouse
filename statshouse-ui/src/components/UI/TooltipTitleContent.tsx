// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';

export type TitleContentProps = {
  children?: React.ReactNode;
};
export const TooltipTitleContent = React.memo(function TooltipTitleContent({ children }: TitleContentProps) {
  if (!children) {
    return null;
  }
  if (typeof children === 'string') {
    return (
      <div className="small text-secondary overflow-auto">
        <div style={{ maxWidth: '80vw', whiteSpace: 'pre-wrap' }}>{children}</div>
        <div className="opacity-0 overflow-hidden h-0" style={{ maxWidth: '80vw', whiteSpace: 'pre' }}>
          {children}
        </div>
      </div>
    );
  }
  return <>{children}</>;
});
