// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { DashboardInfo } from '@/api/dashboard';
import { getDefaultParams } from '@/url2';
import { normalizeDashboard } from './normalizeDashboard';

export function readDataDashboard(data: DashboardInfo, defaultParams = getDefaultParams()) {
  return normalizeDashboard(data, {
    ...defaultParams,
    dashboardId: data.dashboard.dashboard_id?.toString(),
    dashboardName: data.dashboard.name,
    dashboardDescription: data.dashboard.description,
    dashboardVersion: data.dashboard.version,
    dashboardCurrentVersion: data.dashboard.current_version,
  });
}
