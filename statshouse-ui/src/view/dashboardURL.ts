import { GET_PARAMS } from '../api/enum';

export function dashboardURL(id?: number, v?: number | null): string {
  if (!id) {
    return `/api/dashboard`;
  }
  const p = [[GET_PARAMS.dashboardID, id.toString()]];
  if (v != null) {
    p.push([GET_PARAMS.dashboardApiVersion, v.toString()]);
  }
  const strParams = new URLSearchParams(p).toString();
  return `/api/dashboard?${strParams}`;
}
