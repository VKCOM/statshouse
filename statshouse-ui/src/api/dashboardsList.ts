// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { apiFetch, ApiFetchResponse, ExtendedError } from './api';
import { CancelledError, UndefinedInitialDataOptions, useQuery } from '@tanstack/react-query';
import { queryClient } from '../common/queryClient';

const ApiDashboardListEndpoint = '/api/dashboards-list';

/**
 * Response endpoint api/dashboards-list
 */
export type ApiDashboardList = {
  data: GetDashboardListResp;
};

export type GetDashboardListResp = {
  dashboards: DashboardShortInfo[] | null;
};

export type DashboardShortInfo = {
  id: number;
  name: string;
  description: string;
};

export async function apiDashboardListFetch(keyRequest?: unknown) {
  return await apiFetch<ApiDashboardList>({ url: ApiDashboardListEndpoint, keyRequest });
}

export function getDashboardListOptions<T = ApiDashboardList>(): UndefinedInitialDataOptions<
  ApiDashboardList,
  ExtendedError,
  T,
  [string]
> {
  return {
    queryKey: [ApiDashboardListEndpoint],
    queryFn: async ({ signal }) => {
      const { response, error } = await apiDashboardListFetch(signal);
      if (error) {
        throw error;
      }
      if (!response) {
        throw new ExtendedError('empty response');
      }
      return response;
    },
    placeholderData: (previousData) => previousData,
  };
}

export async function apiDashboardList(): Promise<ApiFetchResponse<ApiDashboardList>> {
  const result: ApiFetchResponse<ApiDashboardList> = { ok: false, status: 0 };
  try {
    result.response = await queryClient.fetchQuery(getDashboardListOptions<ApiDashboardList>());
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

export function useApiDashboardList<T = ApiDashboardList>(select?: (response?: ApiDashboardList) => T) {
  const options = getDashboardListOptions();
  return useQuery({
    ...options,
    select,
  });
}
export function selectApiDashboardList(response?: ApiDashboardList) {
  return response?.data.dashboards ?? [];
}
