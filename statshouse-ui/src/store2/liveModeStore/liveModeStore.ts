// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { type StoreSlice } from '../createStore';
import { type StatsHouseStore } from '../statsHouseStore';
import { validPath } from '../helpers';
import { appHistory } from 'common/appHistory';
import { debug } from 'common/debug';
import { TIME_RANGE_KEYS_TO } from 'api/enum';

export type LiveMode = {
  status: boolean;
  interval: number;
  disabled: boolean;
};
export type LiveModeStore = {
  liveMode: LiveMode;
  setLiveMode(status: boolean, disabled?: boolean): void;
};
export const liveModeStore: StoreSlice<StatsHouseStore, LiveModeStore> = (setState, getState, store) => {
  let id: NodeJS.Timeout | undefined = undefined;
  function liveTick() {
    if (document.visibilityState === 'visible' && getState().liveMode.status && validPath(appHistory.location)) {
      getState().setTimeRange({ from: getState().params.timeRange.from, to: TIME_RANGE_KEYS_TO.Now }, true);
    }
  }

  store.subscribe((state, prevState) => {
    const disabled = !state.params.orderPlot.every((plotKey) => state.params.plots[plotKey]?.useV2 ?? true);
    if (state.params.live !== prevState.params.live || disabled !== prevState.liveMode.disabled) {
      getState().setLiveMode(
        state.params.live,
        !state.params.orderPlot.every((plotKey) => state.params.plots[plotKey]?.useV2 ?? true)
      );
    }
    if (!state.liveMode.status || state.liveMode.interval !== prevState.liveMode.interval || id != null) {
      clearInterval(id);
      id = undefined;
      debug.log('live mode disabled');
    }
    if (state.liveMode.status && id == null) {
      debug.log('live mode enabled', state.liveMode.interval);
      liveTick();
      id = setInterval(() => {
        liveTick();
        // todo:
      }, state.liveMode.interval * 1000);
    }
  });
  return {
    liveMode: {
      status: false,
      interval: 300,
      disabled: false,
    },
    setLiveMode(status, disabled) {
      const interval = getLiveModeInterval(getState().params.timeRange.from);
      setState((state) => {
        if (disabled != null) {
          state.liveMode.disabled = disabled;
        }
        state.liveMode.status = status && !state.liveMode.disabled;
        state.liveMode.interval = interval;
        if (!state.liveMode.status && state.params.live) {
          state.params.live = false;
        }
      });
    },
  };
};
export function getLiveModeInterval(relativeFrom: number) {
  return -relativeFrom <= 2 * 3600 ? 1 : -relativeFrom <= 48 * 3600 ? 15 : -relativeFrom <= 31 * 24 * 3600 ? 60 : 300;
}
// useLiveModeStore.subscribe((state, prevState) => {
//   if (!state.status || state.interval !== prevState.interval || id != null) {
//     clearInterval(id);
//     id = undefined;
//     debug.log('live mode disabled');
//   }
//   if (state.status && id == null) {
//     debug.log('live mode enabled', state.interval);
//     liveTick();
//     id = setInterval(() => {
//       liveTick();
//     }, state.interval * 1000);
//   }
// });
//
// useUrlStore?.subscribe((state, prevState) => {
//   if (state.params.live !== prevState.params.live) {
//     setLiveMode(
//       state.params.live,
//       !state.params.orderPlot.every((plotKey) => state.params.plots[plotKey]?.useV2 ?? true)
//     );
//   }
// });
