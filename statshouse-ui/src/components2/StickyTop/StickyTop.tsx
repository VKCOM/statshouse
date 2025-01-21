// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useWindowSize } from '@/hooks/useWindowSize';
import cn from 'classnames';
import { memo } from 'react';
import { HTMLAttributes, ReactNode } from '~@types/react';

interface StickyTopProps extends HTMLAttributes<HTMLDivElement> {
  children: ReactNode;
}

export const StickyTop = memo(function StickyTop({ children, ...props }: StickyTopProps) {
  const scrollY = useWindowSize((s) => s.scrollY > 16);

  return (
    <div className={cn('sticky-top mt-2 bg-body', scrollY && 'shadow-sm small')} {...props}>
      {children}
    </div>
  );
});
