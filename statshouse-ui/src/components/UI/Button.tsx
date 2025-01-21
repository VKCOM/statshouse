// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import { Tooltip } from './Tooltip';

export type ButtonProps = {
  children?: React.ReactNode;
  title?: React.ReactNode;
} & Omit<React.ButtonHTMLAttributes<HTMLButtonElement>, 'title'>;

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(function Button(
  { children, title, ...props }: ButtonProps,
  ref
) {
  return (
    <Tooltip<'button'> {...props} title={title} ref={ref} as="button">
      {children}
    </Tooltip>
  );
});
