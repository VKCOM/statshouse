import { createStore } from '../createStore';
import { resortObjectKey, sumArray } from '../../common/helpers';

const limitHistory = 10;
const changeStatus = (limitHistory / 2) * 0.8;
const maxTimeout = 32;

export type PlotHealsStatus = {
  response: number[];
  status: boolean;
  lastTimestamp: number;
  timout: number;
};

export type PlotHealsStore = {
  status: Record<string, PlotHealsStatus>;
};

export const usePlotHealsStore = createStore<PlotHealsStore>(
  () => ({
    status: {},
    heals: {},
  }),
  'PlotHealsStore'
);

export function getDefaultPlotHealsStatus() {
  return {
    response: new Array(limitHistory).fill(1),
    status: true,
    timout: 0,
    lastTimestamp: Date.now(),
  };
}

export function removePlotHeals(indexPlot: string) {
  usePlotHealsStore.setState((store) => {
    delete store.status[indexPlot];
  });
}

export function addStatus(indexPlot: string, status: boolean) {
  usePlotHealsStore.setState((store) => {
    store.status[indexPlot] ??= getDefaultPlotHealsStatus();
    store.status[indexPlot].response.push(status ? 1 : -1);
    store.status[indexPlot].response = store.status[indexPlot].response.slice(-limitHistory);
    store.status[indexPlot].lastTimestamp = Date.now();
    const sum = sumArray(store.status[indexPlot].response);

    if (store.status[indexPlot].status && sum < -changeStatus) {
      store.status[indexPlot].status = false;
    }
    if (!store.status[indexPlot].status && sum > changeStatus) {
      store.status[indexPlot].status = true;
    }
    if (store.status[indexPlot].status) {
      store.status[indexPlot].timout = 0;
    } else {
      if (status) {
        store.status[indexPlot].timout = Math.max(0, Math.floor((store.status[indexPlot].timout || 1) / 2));
      } else {
        store.status[indexPlot].timout = Math.min(maxTimeout, (store.status[indexPlot].timout || 1) * 2);
      }
    }
  });
}

export function skipRequestPlot(indexPlot: string) {
  const status = usePlotHealsStore.getState().status[indexPlot];
  return status && !status.status && status.lastTimestamp + status.timout * 1000 > Date.now();
}

export function resortPlotHeals(remap: Record<string, string | number>) {
  usePlotHealsStore.setState((state) => {
    state.status = resortObjectKey(state.status, remap);
  });
}
