import {
  getDefaultParams,
  isNotNilVariableLink,
  normalizeFilterKey,
  QueryParams,
  toPlotKey,
  toTagKey,
} from '../url/queryParams';
import { isNotNil } from '../common/helpers';

export interface DashboardMeta {
  dashboard_id?: number;
  name: string;
  description: string;
  version?: number;
  update_time?: number;
  deleted_time?: number;
  data: { [key: string]: unknown };
}

export interface DashboardInfo {
  dashboard: DashboardMeta;
  delete_mark?: boolean;
}

export function normalizeDashboard(data: DashboardInfo): QueryParams {
  const params = data.dashboard.data as QueryParams;
  if (params.dashboard?.groups) {
    params.dashboard.groupInfo = params.dashboard.groupInfo?.map((g, index) => ({
      ...g,
      count:
        params.dashboard?.groups?.reduce((res: number, item) => {
          if (item === index) {
            res = res + 1;
          }
          return res;
        }, 0 as number) ?? 0,
    }));
    delete params.dashboard.groups;
  }
  // @ts-ignore
  const timeShifts = params.timeShifts ?? params.plots[0]?.timeShifts ?? [];
  return {
    ...getDefaultParams(),
    ...params,
    live: getDefaultParams().live,
    theme: getDefaultParams().theme,
    plots: params.plots.map((p, index) => {
      // @ts-ignore
      delete p.timeShifts;
      p.id ??= `${index}`;
      p.customName ??= '';
      p.customDescription ??= '';
      p.promQL ??= '';
      p.events ??= [];
      p.eventsBy ??= [];
      p.eventsHide ??= [];
      p.type ??= 0;
      p.filterIn = normalizeFilterKey(p.filterIn);
      p.filterNotIn = normalizeFilterKey(p.filterNotIn);
      p.groupBy = p.groupBy.map((g) => toTagKey(g)).filter(isNotNil);
      p.metricType ??= undefined;
      p.filledGraph ??= true;
      p.totalLine ??= false;
      return p;
    }),
    timeShifts,
    eventFrom: 0,
    dashboard: {
      ...(params.dashboard ?? {}),
      dashboard_id: data.dashboard.dashboard_id,
      name: data.dashboard.name,
      description: data.dashboard?.description ?? '',
      version: data.dashboard.version,
      groupInfo:
        params.dashboard?.groupInfo?.map((g) => ({
          name: g.name ?? '',
          count: g.count ?? 0,
          show: g.show ?? true,
          size: g.size?.toString?.() ?? '2',
          description: g.description ?? '',
        })) ?? [],
    },
    variables:
      params.variables?.map((v) => ({
        ...v,
        link: v.link.map(([plot, tag]) => [toPlotKey(plot), toTagKey(tag)]).filter(isNotNilVariableLink),
        source: v.source ?? [],
      })) ?? [],
  };
}