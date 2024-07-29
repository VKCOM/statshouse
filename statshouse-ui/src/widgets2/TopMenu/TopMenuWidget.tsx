import React from 'react';

import { PlotNavigate } from 'components2';
import { useStatsHouseShallow } from '../../store2';

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
