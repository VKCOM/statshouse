// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { create, StateCreator } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import { useErrorStore } from '../errors';
import { apiDashboardListFetch, DashboardShortInfo } from '../../api/dashboardsList';

export type DashboardListStore = {
  list: DashboardShortInfo[];
  loading: boolean;
  update(): Promise<void>;
};

export const dashboardListState: StateCreator<
  DashboardListStore,
  [['zustand/immer', never]],
  [],
  DashboardListStore
> = (setState) => {
  let errorRemove: (() => void) | undefined;
  return {
    list: [],
    loading: false,
    async update() {
      if (errorRemove) {
        errorRemove();
        errorRemove = undefined;
      }
      setState((state) => {
        state.loading = true;
      });
      const { response, error } = await apiDashboardListFetch('dashboardListState');
      if (response) {
        setState((state) => {
          state.list = response.data.dashboards ?? [];
        });
      }
      if (error) {
        errorRemove = useErrorStore.getState().addError(error);
      }
      setState((state) => {
        state.loading = false;
      });
    },
  };
};

export const useDashboardListStore = create<DashboardListStore, [['zustand/immer', never]]>(
  immer((...a) => ({
    ...dashboardListState(...a),
  }))
);
