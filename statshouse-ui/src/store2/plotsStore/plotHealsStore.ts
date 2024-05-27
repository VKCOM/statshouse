import { createStore } from '../createStore';
import { sumArray } from '../../common/helpers';
import { PlotKey } from '../urlStore';
import { produce } from 'immer';

const limitHistory = 10;
const changeStatus = (limitHistory / 2) * 0.8;
const maxTimeout = 32;

export type PlotHealsStatus = {
  response: number[];
  status: boolean;
  lastTimestamp: number;
  timeout: number;
};

export type PlotHealsStore = {
  status: Partial<Record<PlotKey, PlotHealsStatus>>;
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

export function removePlotHeals(plotKey: PlotKey) {
  usePlotHealsStore.setState(
    produce((store) => {
      delete store.status[plotKey];
    })
  );
}

export function addStatus(plotKey: PlotKey, status: boolean) {
  usePlotHealsStore.setState(
    produce((store) => {
      store.status[plotKey] ??= getDefaultPlotHealsStatus();
      store.status[plotKey].response.push(status ? 1 : -1);
      store.status[plotKey].response = store.status[plotKey].response.slice(-limitHistory);
      store.status[plotKey].lastTimestamp = Date.now();
      const sum = sumArray(store.status[plotKey].response);

      if (store.status[plotKey].status && sum < -changeStatus) {
        store.status[plotKey].status = false;
      }
      if (!store.status[plotKey].status && sum > changeStatus) {
        store.status[plotKey].status = true;
      }
      if (store.status[plotKey].status) {
        store.status[plotKey].timout = 0;
      } else {
        if (status) {
          store.status[plotKey].timout = Math.max(0, Math.floor((store.status[plotKey].timout || 1) / 2));
        } else {
          store.status[plotKey].timout = Math.min(maxTimeout, (store.status[plotKey].timout || 1) * 2);
        }
      }
    })
  );
}

export function skipRequestPlot(plotKey: PlotKey) {
  const status = usePlotHealsStore.getState().status[plotKey];
  return status && !status.status && status.lastTimestamp + status.timeout * 1000 > Date.now();
}
