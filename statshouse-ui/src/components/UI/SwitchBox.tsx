// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { ChangeEvent, memo, useCallback, useId } from 'react';
import cn from 'classnames';
import { Tooltip } from './Tooltip';

export type SwitchBoxProps = {
  checked?: boolean;
  defaultChecked?: boolean;
  onChange?: (value: boolean) => void;
  children?: React.ReactNode;
  className?: string;
  disabled?: boolean;
  title?: string;
};

export const SwitchBox = memo(function SwitchBox({
  children,
  className,
  defaultChecked,
  checked,
  title,
  onChange,
  disabled,
}: SwitchBoxProps) {
  const uid = useId();
  const change = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      onChange?.(e.currentTarget.checked);
    },
    [onChange]
  );
  return (
    <Tooltip<'div'>
      as="div"
      className={cn('form-check form-switch d-flex align-items-center ps-0', className)}
      title={title}
    >
      <input
        className="form-check-input ms-0"
        type="checkbox"
        checked={checked}
        defaultChecked={defaultChecked}
        role="switch"
        id={`switch-box-${uid}`}
        onChange={change}
        disabled={disabled}
      />
      {!!children && (
        <label className="form-check-label ms-2" htmlFor={`switch-box-${uid}`}>
          {children}
        </label>
      )}
    </Tooltip>
  );
});
