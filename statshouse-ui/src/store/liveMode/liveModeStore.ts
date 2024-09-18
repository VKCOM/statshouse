import { createStore } from '../createStore';
import { useStore } from '../statshouse';
import { debug } from 'common/debug';
import { produce } from 'immer';
import { now } from 'view/utils2';

export type LiveModeStore = {
  live: boolean;
  interval: number;
};

export const useLiveModeStore = createStore<LiveModeStore>((setState, getState, store) => {
  store.subscribe((state, prevState) => {
    if (!state.live || state.interval !== prevState.interval || id != null) {
      clearInterval(id);
      id = undefined;
      debug.log('live mode disabled');
    }
    if (state.live && id == null) {
      debug.log('live mode enabled', state.interval);
      liveTick();
      id = setInterval(() => {
        liveTick();
      }, state.interval * 1000);
    }
  });
  return { live: false, interval: 300 };
}, 'useLiveMove');

let id: NodeJS.Timeout | undefined = undefined;
const liveTick = () => {
  if (
    document.visibilityState === 'visible' &&
    useLiveModeStore.getState().live &&
    (document.location.pathname === '/view' || document.location.pathname === '/embed')
  ) {
    useStore.getState().setTimeRange(
      (range) => ({
        to: range.absolute ? now() : range.getRangeUrl().to,
        from: range.relativeFrom,
      }),
      true
    );
  }
};

export function setLiveMode(status: boolean) {
  const relativeFrom = useStore.getState().timeRange.relativeFrom;
  useLiveModeStore.setState((state) => {
    state.live = status;
    state.interval =
      -relativeFrom <= 2 * 3600 ? 1 : -relativeFrom <= 48 * 3600 ? 15 : -relativeFrom <= 31 * 24 * 3600 ? 60 : 300;
  });
  if (!status && useStore.getState().params.live) {
    useStore.getState().setParams(
      produce((params) => {
        params.live = false;
      })
    );
  }
}

export function setLiveModeInterval(relativeFrom: number) {
  useLiveModeStore.setState((state) => {
    state.interval =
      -relativeFrom <= 2 * 3600 ? 1 : -relativeFrom <= 48 * 3600 ? 15 : -relativeFrom <= 31 * 24 * 3600 ? 60 : 300;
  });
}
