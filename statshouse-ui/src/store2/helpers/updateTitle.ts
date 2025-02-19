// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { pageTitle } from '@/store2';
import { getMetricFullName } from './lib';
import { usePlotsDataStore } from '@/store2/plotDataStore';
import { PlotParams } from '@/url2';

export function updateTitle(tabNum: string, dashboardName: string, plot?: PlotParams) {
  let nextTitle = pageTitle;
  switch (tabNum) {
    case '-1':
      nextTitle = `${dashboardName || 'Dashboard'} — ${pageTitle}`;
      break;
    case '-2':
      nextTitle = `Dashboard setting — ${pageTitle}`;
      break;
    case '-3':
      nextTitle = `Dashboard history — ${pageTitle}`;
      break;
    default: {
      if (tabNum === plot?.id) {
        const fullName = getMetricFullName(plot, usePlotsDataStore.getState().plotsData[plot.id]);
        nextTitle = `${fullName} — ${pageTitle}`;
      }
      break;
    }
  }
  if (document.title !== nextTitle) {
    document.title = nextTitle;
  }
}
