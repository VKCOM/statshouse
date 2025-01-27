// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useQuery } from '@tanstack/react-query';
import { apiFetch } from './api';

export const API_HISTORY = '/api/history';

export type ApiHistory = {
  data: GetHistoryListResponse;
};

export type GetHistoryListResponse = {
  events: HistoryShortInfo[];
};

export type HistoryShortInfo = {
  metadata: HistoryShortInfoMetadata;
  version: number;
  update_time?: number;
};

export type HistoryShortInfoMetadata = {
  user_email: string;
  user_name: string;
  user_ref: string;
};

export async function apHistoryListFetch(id: string, keyRequest?: unknown) {
  return await apiFetch<ApiHistory>({ url: API_HISTORY, get: { id }, keyRequest });
}

export function useHistoryList(id: string) {
  return useQuery({
    queryKey: [API_HISTORY, id],
    queryFn: async ({ signal }) => {
      const { response, error } = await apHistoryListFetch(id, signal);

      if (error) {
        throw error;
      }

      return response?.data.events;
    },
  });
}
