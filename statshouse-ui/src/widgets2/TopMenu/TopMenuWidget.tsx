import React from 'react';
import { useUrlStore } from '../../store2';
export function TopMenuWidget() {
  const { dashboardName, dashboardDescription } = useUrlStore(
    ({ params: { dashboardName, dashboardDescription } }) => ({
      dashboardName,
      dashboardDescription,
    })
  );
  return (
    <div>
      <div>{dashboardName}</div>
      <div>{dashboardDescription}</div>
    </div>
  );
}
