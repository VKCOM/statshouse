import { createStore, StoreSlice } from '../createStore';
import { toNumber } from 'common/helpers';
import { StatsHouseStore } from '../statsHouseStore';

export type TVModeStore = {
  enable: boolean;
  interval: number;
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

export const tvModeStore: StoreSlice<TVModeStore, TVModeStore> = (setState, getState, store) => {
  const interval = getStorageTVInterval();
  store.subscribe((state, prevState) => {
    if (state.interval !== prevState.interval) {
      clearInterval(timer);
      if (state.interval !== prevState.interval) {
        setStorageTVInterval(state.interval);
      }
      if (state.enable && state.interval > 0) {
        timer = setInterval(tickTVMode, state.interval);
      }
      if (state.enable && state.enable !== prevState.enable) {
        lastGroup = undefined;
        tickTVMode();
      }
    }
  });
  return {
    enable: false,
    interval,
  };
};
export function setTVMode({ enable, interval }: { enable?: boolean; interval?: number }) {
  useTvModeStore.setState((state) => {
    if (enable != null) {
      state.enable = enable;
    }
    if (interval != null) {
      state.interval = interval;
    }
  });
}

export const useTvModeStore = createStore<TVModeStore>(tvModeStore);

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
