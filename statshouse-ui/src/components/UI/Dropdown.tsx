// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo } from 'react';
import { Tooltip } from '@/components/UI/Tooltip';
import cn from 'classnames';
import { POPPER_HORIZONTAL, POPPER_VERTICAL } from '@/components/UI/Popper';
import { useStateBoolean } from '@/hooks';

import { DropdownContextProvider } from '@/contexts/DropdownContextProvider';

export type DropdownProps = { className?: string; caption?: React.ReactNode; children?: React.ReactNode };

export const Dropdown = memo(function Dropdown({ className, children, caption }: DropdownProps) {
  const [dropdown, setDropdown] = useStateBoolean(false);

  return (
    <Tooltip
      as="button"
      type="button"
      className={cn(className, 'overflow-auto')}
      title={<DropdownContextProvider value={setDropdown}>{children}</DropdownContextProvider>}
      open={dropdown}
      vertical={POPPER_VERTICAL.outBottom}
      horizontal={POPPER_HORIZONTAL.right}
      onClick={setDropdown.toggle}
      onClickOuter={setDropdown.off}
      titleClassName={'p-0 m-0'}
      noStyle
    >
      {caption}
    </Tooltip>
  );
});
