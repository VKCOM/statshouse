import React, { memo } from 'react';
import { ButtonToggleLiveMode } from '../Plot/PlotNavigate/ButtonToggleLiveMode';
import { ButtonToggleTvMode } from './ButtonToggleTvMode';
import { TvModeInterval } from './TvModeInterval';

export type TvModePanelProps = {
  className?: string;
};

export function _TvModePanel({ className }: TvModePanelProps) {
  return (
    <div className={className}>
      <div className="input-group input-group-sm">
        <ButtonToggleLiveMode className="btn-sm rounded-start-1" />
        <TvModeInterval />
        <ButtonToggleTvMode className="btn-sm" />
      </div>
    </div>
  );
}

export const TvModePanel = memo(_TvModePanel);
