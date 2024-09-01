// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { createStore, type StoreSlice } from '../createStore';
import { type StatsHouseStore, useStatsHouse } from '../statsHouseStore';
import { isValidPath, type ProduceUpdate } from '../helpers';
import { appHistory } from 'common/appHistory';
import { debug } from 'common/debug';
import { TIME_RANGE_KEYS_TO } from 'api/enum';
import { useShallow } from 'zustand/react/shallow';

export type LiveMode = {
  status: boolean;
  interval: number;
  disabled: boolean;
};

export type LiveModeStore = {
  liveMode: LiveMode;
  setLiveMode(status: boolean): void;
};

export const liveModeStore: StoreSlice<LiveModeStore, LiveModeStore> = (setState, getState, store) => {
  let id: NodeJS.Timeout | undefined = undefined;
  function liveTick() {
    if (document.visibilityState === 'visible' && getState().liveMode.status && isValidPath(appHistory.location)) {
      useStatsHouse
        .getState()
        .setTimeRange({ from: useStatsHouse.getState().params.timeRange.from, to: TIME_RANGE_KEYS_TO.Now }, true);
    }
  }

  store.subscribe((state, prevState) => {
    if (state.liveMode.disabled) {
      getState().setLiveMode(false);
    }
    if (
      ((!state.liveMode.status || state.liveMode.interval !== prevState.liveMode.interval) && id != null) ||
      state.liveMode.disabled
    ) {
      clearInterval(id);
      id = undefined;
      debug.log('live mode disabled');
    }
    if (state.liveMode.status && !state.liveMode.disabled && id == null) {
      debug.log('live mode enabled', state.liveMode.interval);
      liveTick();

      id = setInterval(() => {
        liveTick();
      }, state.liveMode.interval * 1000);
    }
  });
  return {
    liveMode: {
      status: false,
      interval: 300,
      disabled: false,
    },
    setLiveMode(status) {
      setState((state) => {
        state.liveMode.status = status && !state.liveMode.disabled;
      });
      if (!getState().liveMode.status && useStatsHouse.getState().params.live) {
        useStatsHouse.getState().setParams((s) => {
          s.live = false;
        });
      }
    },
  };
};
export function getLiveModeInterval(relativeFrom: number) {
  return -relativeFrom <= 2 * 3600 ? 1 : -relativeFrom <= 48 * 3600 ? 15 : -relativeFrom <= 31 * 24 * 3600 ? 60 : 300;
}

export const useLiveModeStore = createStore<LiveModeStore>(liveModeStore);

export function updateLiveMode(state: StatsHouseStore): ProduceUpdate<LiveModeStore> {
  return (s) => {
    s.liveMode.disabled = !state.params.orderPlot.every((plotKey) => state.params.plots[plotKey]?.useV2 ?? true);
    s.liveMode.interval = getLiveModeInterval(state.params.timeRange.from);
    s.liveMode.status = (s.liveMode.status && state.params.timeRange.absolute) || state.params.live;
  };
}

export function useLiveModeStoreShallow<T = unknown>(selector: (state: LiveModeStore) => T): T {
  return useLiveModeStore(useShallow(selector));
}
