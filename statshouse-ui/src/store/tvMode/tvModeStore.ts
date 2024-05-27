import { createStore } from '../createStore';
import { debug } from '../../common/debug';
import { toNumber } from '../../common/helpers';

const fullScreenElement = document.documentElement;

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

export const useTVModeStore = createStore<TVModeStore>((setState, getState, store) => {
  const interval = getStorageTVInterval();
  store.subscribe((state, prevState) => {
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
  });
  return { enable: false, interval };
});

document.addEventListener('fullscreenchange', () => {
  useTVModeStore.setState((state) => {
    state.enable = !!document.fullscreenElement;
  });
});

export function toggleEnableTVMode() {
  if (document.fullscreenEnabled) {
    if (!document.fullscreenElement) {
      fullScreenElement?.requestFullscreen()?.catch(debug.error);
    } else {
      document.exitFullscreen?.()?.catch(debug.error);
    }
  }
}

export function setIntervalTVMode(interval: number) {
  useTVModeStore.setState((state) => {
    state.interval = interval;
  });
}

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
