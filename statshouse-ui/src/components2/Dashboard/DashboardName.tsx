// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo } from 'react';
import { useWindowSize } from 'hooks/useWindowSize';
import cn from 'classnames';
import { Tooltip } from 'components/UI';
import { DashboardNameTitle } from './DashboardNameTitle';
import { useStatsHouseShallow } from 'store2';

export function _DashboardName() {
  const { dashboardName, dashboardDescription } = useStatsHouseShallow(
    ({ params: { dashboardName, dashboardDescription } }) => ({ dashboardName, dashboardDescription })
  );
  const scrollY = useWindowSize((s) => s.scrollY > 16);

  if (!dashboardName) {
    return null;
  }
  return (
    <div className={cn('sticky-top mt-2 bg-body', scrollY && 'shadow-sm small')}>
      <Tooltip
        className="container-xl d-flex flex-row gap-2"
        title={<DashboardNameTitle name={dashboardName} description={dashboardDescription} />}
        hover
        horizontal="left"
      >
        <div>
          {dashboardName}
          {!!dashboardDescription && ':'}
        </div>
        {!!dashboardDescription && (
          <div className="text-secondary flex-grow-1 w-0 overflow-hidden text-truncate">
            <>{dashboardDescription}</>
          </div>
        )}
      </Tooltip>
    </div>
  );
}
export const DashboardName = memo(_DashboardName);
