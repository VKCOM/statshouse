// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { createStore, type StoreSlice } from '../createStore';
import { type StatsHouseStore, useStatsHouse } from '@/store2';
import { isValidPath, type ProduceUpdate } from '../helpers';
import { appHistory } from '@/common/appHistory';
import { debug } from '@/common/debug';
import { METRIC_VALUE_BACKEND_VERSION, TIME_RANGE_KEYS_TO } from '@/api/enum';
import { useShallow } from 'zustand/react/shallow';

export type LiveModeStore = {
  status: boolean;
  interval: number;
  disabled: boolean;
};

export const liveModeStore: StoreSlice<LiveModeStore, LiveModeStore> = (_setState, _getState, store) => {
  let id: NodeJS.Timeout | undefined = undefined;

  store.subscribe((state, prevState) => {
    if (state.disabled) {
      setLiveMode(false);
    }
    if (((!state.status || state.interval !== prevState.interval) && id != null) || state.disabled) {
      clearInterval(id);
      id = undefined;
      debug.log('live mode disabled');
    }
    if (state.status && !state.disabled && id == null) {
      debug.log('live mode enabled', state.interval);
      liveTick();

      id = setInterval(() => {
        liveTick();
      }, state.interval * 1000);
    }
  });
  return {
    status: false,
    interval: 300,
    disabled: false,
  };
};

function liveTick() {
  if (
    document.visibilityState === 'visible' &&
    useLiveModeStore.getState().status &&
    isValidPath(appHistory.location)
  ) {
    useStatsHouse
      .getState()
      .setTimeRange({ from: useStatsHouse.getState().params.timeRange.from, to: TIME_RANGE_KEYS_TO.Now }, true);
  }
}

export function getLiveModeInterval(relativeFrom: number) {
  return -relativeFrom <= 2 * 3600 ? 1 : -relativeFrom <= 48 * 3600 ? 15 : -relativeFrom <= 31 * 24 * 3600 ? 60 : 300;
}

export const useLiveModeStore = createStore<LiveModeStore>(liveModeStore);

export function updateLiveMode(state: StatsHouseStore): ProduceUpdate<LiveModeStore> {
  return (s) => {
    s.disabled = !Object.keys(state.params.plots).every(
      (plotKey) => state.params.plots[plotKey]?.backendVersion !== METRIC_VALUE_BACKEND_VERSION.v1
    );
    s.interval = getLiveModeInterval(state.params.timeRange.from);
    s.status = (s.status && !state.params.timeRange.absolute) || state.params.live;
  };
}

export function useLiveModeStoreShallow<T = unknown>(selector: (state: LiveModeStore) => T): T {
  return useLiveModeStore(useShallow(selector));
}

export function setLiveMode(status: boolean) {
  useLiveModeStore.setState((state) => {
    state.status = status && !state.disabled;
  });
  if (!useLiveModeStore.getState().status && useStatsHouse.getState().params.live) {
    useStatsHouse.getState().setParams((s) => {
      s.live = false;
    });
  }
}
