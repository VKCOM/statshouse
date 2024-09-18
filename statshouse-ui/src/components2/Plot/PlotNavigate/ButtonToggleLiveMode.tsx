// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo } from 'react';
import cn from 'classnames';
import { ToggleButton } from 'components/UI';
import { ReactComponent as SVGPlayFill } from 'bootstrap-icons/icons/play-fill.svg';
import { setLiveMode, useLiveModeStoreShallow } from 'store2/liveModeStore';

export type ButtonToggleLiveModeProps = { className?: string };

export function _ButtonToggleLiveMode({ className }: ButtonToggleLiveModeProps) {
  const { status, disabled } = useLiveModeStoreShallow(({ status, disabled }) => ({
    status,
    disabled,
  }));
  return (
    <ToggleButton
      className={cn('btn btn-outline-primary', !status && 'bg-body', className)}
      title="Follow live"
      checked={status}
      onChange={setLiveMode}
      disabled={disabled}
    >
      <SVGPlayFill />
    </ToggleButton>
  );
}

export const ButtonToggleLiveMode = memo(_ButtonToggleLiveMode);
