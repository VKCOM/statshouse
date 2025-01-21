// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { StatsHouseStore } from '@/store2';
import { pageTitle } from '@/store2';
import { getMetricFullName } from './lib';

export function updateTitle({ params: { tabNum, plots, dashboardName }, plotsData }: StatsHouseStore) {
  switch (tabNum) {
    case '-1':
      document.title = `${dashboardName || 'Dashboard'} — ${pageTitle}`;
      break;
    case '-2':
      document.title = `Dashboard setting — ${pageTitle}`;
      break;
    default: {
      const fullName = getMetricFullName(plots[tabNum], plotsData[tabNum]);
      document.title = `${fullName} — ${pageTitle}`;
      break;
    }
  }
}
