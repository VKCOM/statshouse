import React, { memo } from 'react';
import { useWindowSize } from 'hooks';
import cn from 'classnames';
import { Tooltip } from 'components';
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
          <div className="text-secondary flex-grow-1 w-0 overflow-hidden text-truncate">{dashboardDescription}</div>
        )}
      </Tooltip>
    </div>
  );
}
export const DashboardName = memo(_DashboardName);
