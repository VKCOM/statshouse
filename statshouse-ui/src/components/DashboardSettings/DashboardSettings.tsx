// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import { DashboardInfo } from './DashboardInfo';
import { DashboardTagSync } from './DashboardTagSync';
import { selectorDevEnabled, useStoreDev } from '../../store';
import { DashboardVariable } from './DashboardVariable';

export type DashboardSettingsProps = {};
export const DashboardSettings: React.FC<DashboardSettingsProps> = () => {
  const devEnabled = useStoreDev(selectorDevEnabled);
  return (
    <div className="w-max-720 mx-auto">
      <div className="">
        <div className="mb-4">
          <DashboardInfo />
        </div>
        <div className="mb-4">
          <DashboardVariable />
        </div>
        {devEnabled && (
          <div className="mb-4">
            <DashboardTagSync />
          </div>
        )}
      </div>
    </div>
  );
};
