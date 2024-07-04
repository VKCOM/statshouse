import React, { memo, useCallback } from 'react';
import cn from 'classnames';
import { ToggleButton } from 'components';
import { useStatsHouseShallow } from 'store2';
import { ReactComponent as SVGFullscreen } from 'bootstrap-icons/icons/fullscreen.svg';
import { ReactComponent as SVGFullscreenExit } from 'bootstrap-icons/icons/fullscreen-exit.svg';

export type ButtonToggleTvModeProps = {
  className?: string;
};
export function _ButtonToggleTvMode({ className }: ButtonToggleTvModeProps) {
  const { enable, setTVMode } = useStatsHouseShallow(({ tvMode: { enable }, setTVMode }) => ({
    enable,
    setTVMode,
  }));
  const onToggle = useCallback(
    (status: boolean) => {
      setTVMode({ enable: status });
    },
    [setTVMode]
  );
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
