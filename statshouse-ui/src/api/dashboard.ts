// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { GET_PARAMS } from './enum';
import { apiFetch, ApiFetchResponse, ExtendedError } from './api';
import {
  CancelledError,
  type QueryClient,
  type UndefinedInitialDataOptions,
  useMutation,
  type UseMutationOptions,
  useQuery,
  useQueryClient,
} from '@tanstack/react-query';
import { queryClient } from '@/common/queryClient';
import { type QueryParams, urlEncode } from '@/url2';
import { dashboardMigrateSaveToOld } from '@/store2/urlStore/dashboardMigrate';
import { toNumber } from '@/common/helpers';
import { API_HISTORY } from './history';

const ApiDashboardEndpoint = '/api/dashboard';

/**
 * Response endpoint api/dashboard
 */
export type ApiDashboard = {
  data: DashboardInfo;
};

/**
 * Get params endpoint api/dashboard
 */
export type ApiDashboardGet = {
  [GET_PARAMS.dashboardID]: string;
  [GET_PARAMS.dashboardApiVersion]?: string;
};

/**
 * Post params endpoint api/dashboard
 */
export type ApiDashboardPost = DashboardInfo;

/**
 * Put params endpoint api/dashboard
 */
export type ApiDashboardPut = DashboardInfo;

export type DashboardInfo = {
  dashboard: DashboardMetaInfo;
  delete_mark?: boolean;
};

export type DashboardMetaInfo = {
  dashboard_id?: number;
  name: string;
  description: string;
  version?: number;
  current_version?: number;
  update_time?: number;
  deleted_time?: number;
  data: Record<string, unknown>;
};

export async function apiDashboardFetch(params: ApiDashboardGet, keyRequest?: unknown) {
  return await apiFetch<ApiDashboard>({ url: ApiDashboardEndpoint, get: params, keyRequest });
}

export async function apiDashboardSaveFetch(params: ApiDashboardPost, keyRequest?: unknown) {
  return await apiFetch<ApiDashboard>({
    url: ApiDashboardEndpoint,
    method: params.dashboard.dashboard_id == null ? 'PUT' : 'POST',
    post: params,
    keyRequest,
  });
}

export function getDashboardOptions<T = ApiDashboard>(
  dashboardId: string,
  dashboardVersion?: string
): UndefinedInitialDataOptions<ApiDashboard, ExtendedError, T, [string, ApiDashboardGet]> {
  const fetchParams: ApiDashboardGet = { [GET_PARAMS.dashboardID]: dashboardId };
  if (dashboardVersion != null) {
    fetchParams[GET_PARAMS.dashboardApiVersion] = dashboardVersion;
  }

  return {
    queryKey: [ApiDashboardEndpoint, fetchParams],
    queryFn: async ({ signal }) => {
      const { response, error } = await apiDashboardFetch(fetchParams, signal);

      if (error) {
        throw error;
      }
      if (!response) {
        throw new ExtendedError('empty response');
      }

      if (dashboardVersion != null) {
        const baseParams = { [GET_PARAMS.dashboardID]: dashboardId };

        const cacheKey = [ApiDashboardEndpoint, baseParams];
        const cacheData = queryClient.getQueryData<ApiDashboard>(cacheKey);

        if (!cacheData) {
          const { response: resCurrent } = await apiDashboardFetch(baseParams, signal);

          if (resCurrent) {
            response.data.dashboard.current_version = resCurrent.data.dashboard.version;
            queryClient.setQueryData(cacheKey, resCurrent);
          }
        } else {
          response.data.dashboard.current_version = cacheData.data.dashboard.version;
        }
      }

      return response;
    },
    placeholderData: (previousData) => previousData,
  };
}

export async function apiDashboard(dashboardId: string, dashboardVersion?: string) {
  const result: ApiFetchResponse<ApiDashboard> = { ok: false, status: 0 };
  try {
    result.response = await queryClient.fetchQuery(getDashboardOptions<ApiDashboard>(dashboardId, dashboardVersion));
    result.ok = true;
  } catch (error) {
    result.status = ExtendedError.ERROR_STATUS_UNKNOWN;
    if (error instanceof ExtendedError) {
      result.error = error;
      result.status = error.status;
    } else if (error instanceof CancelledError) {
      result.error = new ExtendedError(error, ExtendedError.ERROR_STATUS_ABORT);
      result.status = ExtendedError.ERROR_STATUS_ABORT;
    } else {
      result.error = new ExtendedError(error);
    }
  }
  return result;
}

export function useApiDashboard<T = ApiDashboard>(
  dashboardId: string,
  dashboardVersion?: string,
  select?: (response?: ApiDashboard) => T,
  enabled: boolean = true
) {
  const options = getDashboardOptions<ApiDashboard>(dashboardId, dashboardVersion);
  return useQuery({ ...options, select, enabled });
}
export function getDashboardSaveFetchParams(params: QueryParams, remove?: boolean, copy?: boolean): DashboardInfo {
  const searchParams = urlEncode(params);

  const oldDashboardParams = dashboardMigrateSaveToOld(params);
  oldDashboardParams.dashboard.data.searchParams = searchParams;
  const version = params.dashboardCurrentVersion || params.dashboardVersion;

  const dashboardParams: DashboardInfo = {
    dashboard: {
      name: params.dashboardName,
      description: params.dashboardDescription,
      version: version ?? 0,
      dashboard_id: toNumber(params.dashboardId) ?? undefined,
      data: {
        ...oldDashboardParams.dashboard.data,
        searchParams,
      },
    },
  };
  if (remove) {
    dashboardParams.delete_mark = true;
  } else if (copy) {
    delete dashboardParams.dashboard.dashboard_id;
    delete dashboardParams.dashboard.version;
  }

  return dashboardParams;
}

export function getDashboardSaveOptions(
  queryClient: QueryClient,
  remove?: boolean,
  copy?: boolean
): UseMutationOptions<ApiDashboard, Error, QueryParams, unknown> {
  return {
    retry: false,
    mutationFn: async (params: QueryParams) => {
      const dashboardParams: DashboardInfo = getDashboardSaveFetchParams(params, remove, copy);
      const { response, error } = await apiDashboardSaveFetch(dashboardParams);

      if (error) {
        throw error;
      }
      if (!response) {
        throw new ExtendedError('empty response');
      }
      return response;
    },

    onSuccess: (data) => {
      const dashboardId = data.data.dashboard.dashboard_id?.toString();
      if (dashboardId) {
        const baseParams = { [GET_PARAMS.dashboardID]: dashboardId };

        queryClient.invalidateQueries({ queryKey: [ApiDashboardEndpoint] });
        queryClient.setQueryData([ApiDashboardEndpoint, baseParams], data);
        queryClient.invalidateQueries({ queryKey: [API_HISTORY, dashboardId], type: 'all' });
      }
    },
  };
}

export async function apiDashboardSave(
  params: QueryParams,
  remove?: boolean,
  copy?: boolean
): Promise<ApiFetchResponse<ApiDashboard>> {
  const options = getDashboardSaveOptions(queryClient, remove, copy);
  const result: ApiFetchResponse<ApiDashboard> = { ok: false, status: 0 };
  try {
    result.response = await options.mutationFn?.(params);
    if (result.response) {
      result.ok = true;
      options.onSuccess?.(result.response, params, undefined);
    } else {
      result.error = new ExtendedError('empty response');
      result.status = ExtendedError.ERROR_STATUS_UNKNOWN;
    }
  } catch (error) {
    result.status = ExtendedError.ERROR_STATUS_UNKNOWN;
    if (error instanceof ExtendedError) {
      result.error = error;
      result.status = error.status;
    } else if (error instanceof CancelledError) {
      result.error = new ExtendedError(error, ExtendedError.ERROR_STATUS_ABORT);
      result.status = ExtendedError.ERROR_STATUS_ABORT;
    } else {
      result.error = new ExtendedError(error);
    }
  }
  return result;
}

export function useApiDashboardSave(remove?: boolean) {
  const queryClient = useQueryClient();
  return useMutation(getDashboardSaveOptions(queryClient, remove));
}
