// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { ChangeEvent, useCallback, useId } from 'react';
import { Tooltip } from './Tooltip';

export type ToggleButtonProps<T> = {
  checked?: boolean;
  defaultChecked?: boolean;
  onChange?: (status: boolean, value?: T) => void;
  children?: React.ReactNode;
  className?: string;
  title?: string;
  value?: T;
  disabled?: boolean;
};

export function ToggleButton<T = unknown>({
  checked,
  disabled,
  defaultChecked,
  onChange,
  children,
  className,
  title,
  value,
}: ToggleButtonProps<T>) {
  const uid = useId();
  const change = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      onChange?.(e.currentTarget.checked, value);
    },
    [onChange, value]
  );
  return (
    <>
      <input
        type="checkbox"
        className="btn-check"
        id={`toggle-button-${uid}`}
        autoComplete="off"
        checked={checked}
        defaultChecked={defaultChecked}
        onChange={change}
        disabled={disabled}
      />
      {!!children && (
        <Tooltip as="label" className={className} htmlFor={`toggle-button-${uid}`} title={title}>
          {children}
        </Tooltip>
      )}
    </>
  );
}
