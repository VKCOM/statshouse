// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { PlotNavigate } from '@/components2';
import { useStatsHouseShallow } from '@/store2';

export function TopMenuWidget() {
  const { dashboardName, dashboardDescription, tabNum } = useStatsHouseShallow(
    ({ params: { dashboardName, dashboardDescription, tabNum } }) => ({
      tabNum,
      dashboardName,
      dashboardDescription,
    })
  );
  return (
    <div>
      <PlotNavigate plotKey={tabNum} />
      <div>{dashboardName}</div>
      <div>{dashboardDescription}</div>
    </div>
  );
}
