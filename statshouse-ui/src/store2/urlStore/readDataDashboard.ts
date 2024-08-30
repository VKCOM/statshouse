import { DashboardInfo } from 'api/dashboard';
import { getDefaultParams } from 'url2';
import { normalizeDashboard } from './normalizeDashboard';

export function readDataDashboard(data: DashboardInfo, defaultParams = getDefaultParams()) {
  return normalizeDashboard(data, {
    ...defaultParams,
    dashboardId: data.dashboard.dashboard_id?.toString(),
    dashboardName: data.dashboard.name,
    dashboardDescription: data.dashboard.description,
    dashboardVersion: data.dashboard.version,
  });
}
