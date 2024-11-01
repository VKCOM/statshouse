// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import Markdown from 'react-markdown';

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
      {!!description && (
        <>
          <div style={{ maxWidth: '80vw', whiteSpace: 'pre-wrap' }}>
            <Markdown>{description}</Markdown>
          </div>
          <div className="opacity-0 overflow-hidden h-0" style={{ maxWidth: '80vw', whiteSpace: 'pre' }}>
            <Markdown>{description}</Markdown>
          </div>
        </>
      )}
    </div>
  );
}
