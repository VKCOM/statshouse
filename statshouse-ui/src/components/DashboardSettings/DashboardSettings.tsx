// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import { DashboardInfo } from './DashboardInfo';
import { DashboardTagSync } from './DashboardTagSync';
import { selectorParamsPlots, useStore } from '../../store';

export type DashboardSettingsProps = {};
export const DashboardSettings: React.FC<DashboardSettingsProps> = () => {
  const plots = useStore(selectorParamsPlots);

  return (
    <div className="w-max-720 mx-auto">
      <div className="">
        <div className="mb-4">
          <DashboardInfo />
        </div>
        {plots.length > 1 && (
          <div className="mb-4">
            <DashboardTagSync />
          </div>
        )}
      </div>
    </div>
  );
};
