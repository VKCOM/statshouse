import type { StatsHouseStore } from '../statsHouseStore';

import { pageTitle } from '../constants';
import { getMetricFullName } from './lib';

export function updateTitle({ params: { tabNum, plots, dashboardName }, plotsData }: StatsHouseStore) {
  switch (tabNum) {
    case '-1':
      document.title = `${dashboardName || 'Dashboard'} — ${pageTitle}`;
      break;
    case '-2':
      document.title = `Dashboard setting — ${pageTitle}`;
      break;
    default:
      const fullName = getMetricFullName(plots[tabNum], plotsData[tabNum]);
      document.title = `${fullName} — ${pageTitle}`;
      break;
  }
}
