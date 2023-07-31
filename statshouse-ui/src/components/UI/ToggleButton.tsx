// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { ChangeEvent, memo, useCallback, useId } from 'react';

export type ToggleButtonProps = {
  checked?: boolean;
  defaultChecked?: boolean;
  onChange?: (value: boolean) => void;
  children?: React.ReactNode;
  className?: string;
  title?: string;
};

export function _ToggleButton({ checked, defaultChecked, onChange, children, className, title }: ToggleButtonProps) {
  const uid = useId();
  const change = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      onChange?.(e.currentTarget.checked);
    },
    [onChange]
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
      />
      {!!children && (
        <label className={className} htmlFor={`toggle-button-${uid}`} title={title}>
          {children}
        </label>
      )}
    </>
  );
}

export const ToggleButton = memo(_ToggleButton);
