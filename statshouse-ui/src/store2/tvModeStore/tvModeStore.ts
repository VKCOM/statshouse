import { StoreSlice } from '../createStore';
import { toNumber } from 'common/helpers';
import { StatsHouseStore } from '../statsHouseStore';

export type TVMode = { enable: boolean; interval: number };
export type TVModeStore = {
  tvMode: TVMode;
  setTVMode: (mode: Partial<TVMode>) => void;
};

export const defaultInterval = 0;
const localStorageParamName = 'tv_interval';
const firstSelectorElement = '.dashLayout';
let timer: NodeJS.Timeout;
let nextGroup: Element | undefined;
let lastGroup: Element | undefined;

export function getStorageTVInterval(): number {
  return toNumber(window.localStorage.getItem(localStorageParamName), defaultInterval);
}

export function setStorageTVInterval(tvInterval: number) {
  if (tvInterval === defaultInterval) {
    window.localStorage.removeItem(localStorageParamName);
  } else {
    window.localStorage.setItem(localStorageParamName, tvInterval.toString());
  }
}

export const tvModeStore: StoreSlice<StatsHouseStore, TVModeStore> = (setState, getState, store) => {
  const interval = getStorageTVInterval();
  store.subscribe((state, prevState) => {
    if (state.tvMode !== prevState.tvMode) {
      if (state.tvMode.interval !== prevState.tvMode.interval) {
        clearInterval(timer);
        if (state.tvMode.interval !== prevState.tvMode.interval) {
          setStorageTVInterval(state.tvMode.interval);
        }
        if (state.tvMode.enable && state.tvMode.interval > 0) {
          timer = setInterval(tickTVMode, state.tvMode.interval);
        }
        if (state.tvMode.enable && state.tvMode.enable !== prevState.tvMode.enable) {
          lastGroup = undefined;
          tickTVMode();
        }
      }
    }
  });
  return {
    tvMode: {
      enable: false,
      interval,
    },
    setTVMode({ enable, interval }) {
      setState((state) => {
        if (enable != null) {
          state.tvMode.enable = enable;
        }
        if (interval != null) {
          state.tvMode.interval = interval;
        }
      });
    },
  };
};

export function tickTVMode() {
  const groups = [...document.querySelectorAll(`div.groupShow[data-group]`)];
  nextGroup = undefined;
  if (!lastGroup) {
    document.querySelector(firstSelectorElement)?.scrollIntoView({ block: 'start', behavior: 'auto' });
    lastGroup = groups[0];
    return;
  }
  lastGroup ??= groups[0];
  if (lastGroup) {
    nextGroup = groups[groups.indexOf(lastGroup) + 1];
  }
  nextGroup ??= groups[0];
  nextGroup?.scrollIntoView({ block: 'start', behavior: 'auto' });
  lastGroup = nextGroup;
}
