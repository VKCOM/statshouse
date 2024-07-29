import React, { memo } from 'react';
import cn from 'classnames';
import { ToggleButton } from 'components';
import { useStatsHouseShallow } from 'store2';
import { ReactComponent as SVGPlayFill } from 'bootstrap-icons/icons/play-fill.svg';

export type ButtonToggleLiveModeProps = { className?: string };

export function _ButtonToggleLiveMode({ className }: ButtonToggleLiveModeProps) {
  const { status, disabled, setLiveMode } = useStatsHouseShallow(({ liveMode: { status, disabled }, setLiveMode }) => ({
    status,
    disabled,
    setLiveMode,
  }));
  return (
    <ToggleButton
      className={cn('btn btn-outline-primary', className)}
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
