// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { create, StateCreator } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import { apiMetricListFetch, MetricShortInfo } from '../../api/metricList';
import { useErrorStore } from '../errors';

export type MetricsListStore = {
  list: MetricShortInfo[];
  loading: boolean;
  update(): Promise<void>;
};

export const metricsListState: StateCreator<MetricsListStore, [['zustand/immer', never]], [], MetricsListStore> = (
  setState
) => {
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
      const { response, error } = await apiMetricListFetch('metricsListState');
      if (response) {
        setState((state) => {
          state.list = response.data.metrics.map((m) => ({ name: m.name, value: m.name }));
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

export const useMetricsListStore = create<MetricsListStore, [['zustand/immer', never]]>(
  immer((...a) => ({
    ...metricsListState(...a),
  }))
);

useMetricsListStore.getState().update();
