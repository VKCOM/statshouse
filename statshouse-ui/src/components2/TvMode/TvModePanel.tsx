// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo } from 'react';
import { ButtonToggleLiveMode } from '../Plot/PlotNavigate/ButtonToggleLiveMode';
import { ButtonToggleTvMode, TvModeInterval } from '@/components2';

export type TvModePanelProps = {
  className?: string;
};

export const TvModePanel = memo(function TvModePanel({ className }: TvModePanelProps) {
  return (
    <div className={className}>
      <div className="input-group input-group-sm">
        <ButtonToggleLiveMode className="btn-sm rounded-start-1" />
        <TvModeInterval />
        <ButtonToggleTvMode className="btn-sm" />
      </div>
    </div>
  );
});
