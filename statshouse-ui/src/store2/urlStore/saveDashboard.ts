import { type QueryParams, urlEncode } from 'url2';
import { dashboardMigrateSaveToOld } from './dashboardMigrate';
import { apiDashboardSaveFetch, DashboardInfo } from '../../api/dashboard';
import { toNumber } from '../../common/helpers';

export async function saveDashboard(params: QueryParams) {
  const searchParams = urlEncode(params);
  const oldDashboardParams = dashboardMigrateSaveToOld(params);
  oldDashboardParams.dashboard.data.searchParams = searchParams;
  const dashboardParams: DashboardInfo = {
    dashboard: {
      name: params.dashboardName,
      description: params.dashboardDescription,
      version: params.dashboardVersion,
      dashboard_id: toNumber(params.dashboardId) ?? undefined,
      data: {
        ...oldDashboardParams.dashboard.data,
        searchParams,
      },
    },
  };
  return apiDashboardSaveFetch(dashboardParams);
}
