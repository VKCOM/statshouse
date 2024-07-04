// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { createStore, type Store } from './createStore';
import { urlStore, type UrlStore } from './urlStore';
import { useShallow } from 'zustand/react/shallow';
import { userStore, type UserStore } from './userStore';
import { plotsInfoStore, type PlotsInfoStore } from './plotsInfoStore';
import { liveModeStore, type LiveModeStore } from './liveModeStore';
import { metricMetaStore, MetricMetaStore } from './metricsMetaStore';
import { plotVisibilityStore, type PlotVisibilityStore } from './plotVisibilityStore';
import { plotPreviewStore, type PlotPreviewStore } from './plotPreviewStore';
import { plotHealsStore, type PlotHealsStore } from './plotHealsStore';
import { plotsDataStore, PlotsDataStore } from './plotDataStore';
import { tvModeStore, TVModeStore } from './tvModeStore';

export type StatsHouseStore = UrlStore &
  UserStore &
  PlotsInfoStore &
  LiveModeStore &
  MetricMetaStore &
  PlotVisibilityStore &
  PlotPreviewStore &
  PlotHealsStore &
  PlotsDataStore &
  TVModeStore;

const statsHouseStore: Store<StatsHouseStore> = (...props) => ({
  ...urlStore(...props),
  ...userStore(...props),
  ...plotsInfoStore(...props),
  ...liveModeStore(...props),
  ...metricMetaStore(...props),
  ...plotVisibilityStore(...props),
  ...plotPreviewStore(...props),
  ...plotHealsStore(...props),
  ...plotsDataStore(...props),
  ...tvModeStore(...props),
});
export const useStatsHouse = createStore<StatsHouseStore>(statsHouseStore);

export function useStatsHouseShallow<T = unknown>(selector: (state: StatsHouseStore) => T): T {
  return useStatsHouse(useShallow(selector));
}
