// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';

import css from './style.module.css';
import cn from 'classnames';
import { Tooltip } from '@/components/UI';

export type AlignByDotProps = {
  value: string;
  title?: React.ReactNode;
  unit?: string;
  className?: string;
};

export const AlignByDot: React.FC<AlignByDotProps> = ({ value, title, unit = '', className }) => {
  const [i, f = ''] = value.replace(unit, '').split('.', 2);
  return (
    <Tooltip<'span'> as="span" className={cn(className, 'font-monospace')} title={title}>
      <span>{i}</span>
      <span className={cn(css.dotSpace)}>
        {f && '.'}
        {f}
        {!!value && unit}
      </span>
    </Tooltip>
  );
};
