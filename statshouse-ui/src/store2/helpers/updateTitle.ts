// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { pageTitle } from '@/store2';
import { getMetricFullName } from './lib';
import type { PlotData } from '@/store2/plotDataStore';
import { QueryParams } from '@/url2';

export function updateTitle({ tabNum, plots, dashboardName }: QueryParams, plotData?: PlotData) {
  let nextTitle = '';
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
      const fullName = getMetricFullName(plots[tabNum], plotData);
      nextTitle = `${fullName} — ${pageTitle}`;
      break;
    }
  }
  if (document.title !== nextTitle) {
    document.title = nextTitle;
  }
}
