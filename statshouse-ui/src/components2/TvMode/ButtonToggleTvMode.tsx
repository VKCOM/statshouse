// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback } from 'react';
import cn from 'classnames';
import { ToggleButton } from 'components/UI';
import { ReactComponent as SVGFullscreen } from 'bootstrap-icons/icons/fullscreen.svg';
import { ReactComponent as SVGFullscreenExit } from 'bootstrap-icons/icons/fullscreen-exit.svg';
import { setTVMode, useTvModeStore } from 'store2/tvModeStore';

export type ButtonToggleTvModeProps = {
  className?: string;
};
export function _ButtonToggleTvMode({ className }: ButtonToggleTvModeProps) {
  const enable = useTvModeStore(({ enable }) => enable);

  const onToggle = useCallback((status: boolean) => {
    setTVMode({ enable: status });
  }, []);
  return (
    <ToggleButton
      className={cn('btn btn-outline-primary', className)}
      checked={enable}
      onChange={onToggle}
      title={enable ? 'TV mode Off' : 'TV mode On'}
    >
      {enable ? <SVGFullscreenExit /> : <SVGFullscreen />}
    </ToggleButton>
  );
}
export const ButtonToggleTvMode = memo(_ButtonToggleTvMode);
