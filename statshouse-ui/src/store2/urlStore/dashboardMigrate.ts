import { DashboardInfo } from 'api/dashboard';
import {
  type DashboardParams as OldDashboardParams,
  encodeParams,
  PLOT_TYPE as OLD_PLOT_TYPE,
  type PlotParams as OldPlotParams,
  type QueryParams as OldQueryParams,
  type VariableParams as OldVariableParams,
} from 'url/queryParams';
import { type PlotKey, type QueryParams } from 'url2';
import { deepClone, toNumber } from '../../common/helpers';
import { METRIC_TYPE, METRIC_VALUE_BACKEND_VERSION, PLOT_TYPE } from '../../api/enum';
import { normalizeDashboard as normalizeDashboardOld } from '../../view/normalizeDashboard';

export function dashboardMigrate(data: unknown) {
  return encodeParams(normalizeDashboardOld(data as DashboardInfo));
}

export function dashboardMigrateNewToOld(params: QueryParams): OldQueryParams {
  const mapPlotIndex: Record<PlotKey, number> = params.orderPlot.reduce(
    (res, pK, index) => {
      res[pK] = index;
      return res;
    },
    {} as Record<PlotKey, number>
  );
  const plots: OldPlotParams[] = params.orderPlot.map((pK, index) => {
    const plot: OldPlotParams = {
      id: index.toString(),
      metricName: params.plots[pK]?.metricName ?? '',
      customName: params.plots[pK]?.customName ?? '',
      customDescription: params.plots[pK]?.customDescription ?? '',
      promQL: params.plots[pK]?.promQL ?? '',
      metricType: params.plots[pK]?.metricUnit ?? METRIC_TYPE.none,
      what: params.plots[pK]?.what.map((w) => w) ?? [],
      customAgg: params.plots[pK]?.customAgg ?? 0,
      groupBy: params.plots[pK]?.groupBy.map((gb) => gb) ?? [],
      filterIn: deepClone(params.plots[pK]?.filterIn ?? {}),
      filterNotIn: deepClone(params.plots[pK]?.filterNotIn ?? {}),
      numSeries: params.plots[pK]?.numSeries ?? 5,
      useV2: params.plots[pK]?.backendVersion === METRIC_VALUE_BACKEND_VERSION.v2,
      yLock: {
        min: params.plots[pK]?.yLock.min ?? 0,
        max: params.plots[pK]?.yLock.max ?? 0,
      },
      maxHost: params.plots[pK]?.maxHost ?? false,
      type: params.plots[pK]?.type === PLOT_TYPE.Event ? OLD_PLOT_TYPE.Event : OLD_PLOT_TYPE.Metric,
      events: params.plots[pK]?.events.map((e) => mapPlotIndex[e]) ?? [],
      eventsBy: params.plots[pK]?.events.map((e) => e) ?? [],
      eventsHide: params.plots[pK]?.events.map((e) => e) ?? [],
      totalLine: params.plots[pK]?.totalLine ?? false,
      filledGraph: params.plots[pK]?.filledGraph ?? true,
    };
    return plot;
  });
  const variables: OldVariableParams[] = params.orderVariables.map((vK) => {
    const variable: OldVariableParams = {
      name: params.variables[vK]?.name ?? '',
      description: params.variables[vK]?.description ?? '',
      source:
        params.variables[vK]?.sourceOrder.map((sK) => ({
          tag: params.variables[vK]?.source[sK]?.tag ?? '0',
          metric: params.variables[vK]?.source[sK]?.metric ?? '',
          filterIn: deepClone(params.variables[vK]?.source[sK]?.filterIn ?? {}),
          filterNotIn: deepClone(params.variables[vK]?.source[sK]?.filterNotIn ?? {}),
        })) ?? [],
      link: params.variables[vK]?.link?.map(([pK, tK]) => [mapPlotIndex[pK].toString(), tK]) ?? [],
      values: params.variables[vK]?.values.map((v) => v) ?? [],
      args: {
        groupBy: params.variables[vK]?.groupBy ?? false,
        negative: params.variables[vK]?.negative ?? false,
      },
    };
    return variable;
  });
  const dashboard: OldDashboardParams = {
    dashboard_id: toNumber(params.dashboardId) ?? undefined,
    name: params.dashboardName,
    description: params.dashboardDescription,
    version: params.dashboardVersion,
    groupInfo: params.orderGroup.map((gK) => ({
      name: params.groups[gK]?.name ?? '',
      description: params.groups[gK]?.description ?? '',
      count: params.groups[gK]?.count ?? 0,
      size: params.groups[gK]?.size ?? '2',
      show: params.groups[gK]?.show ?? true,
    })),
  };
  return {
    timeRange: { from: params.timeRange.from, to: params.timeRange.urlTo },
    live: false,
    tabNum: -1,
    plots,
    timeShifts: [...params.timeShifts],
    variables,
    eventFrom: 0,
    dashboard,
    tagSync: [],
  };
}

export function dashboardMigrateSaveToOld(params: QueryParams) {
  const oldParams = dashboardMigrateNewToOld(params);
  const paramsDashboard: DashboardInfo = {
    dashboard: {
      dashboard_id: oldParams.dashboard?.dashboard_id,
      name: oldParams.dashboard?.name ?? '',
      description: oldParams.dashboard?.description ?? '',
      version: oldParams.dashboard?.version ?? 0,
      data: { ...oldParams, searchParams: encodeParams(oldParams) },
    },
  };
  return paramsDashboard;
}
