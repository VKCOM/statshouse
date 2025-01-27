// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo } from 'react';
import { DashboardInfo } from './DashboardInfo';
import { DashboardVariable } from './DashboardVariable';

export type DashboardSettingsProps = {
  className?: string;
};

export const DashboardSettings = memo(function DashboardSettings() {
  return (
    <div className="w-max-720 mx-auto">
      <div className="mb-4">
        <DashboardInfo />
      </div>
      <div className="mb-4">{<DashboardVariable />}</div>
    </div>
  );
});
