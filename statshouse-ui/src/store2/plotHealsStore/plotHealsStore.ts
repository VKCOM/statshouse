// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { StoreSlice } from '../createStore';
import { StatsHouseStore } from '@/store2';
import { PlotKey } from '@/url2';
import { sumArray } from '@/common/helpers';

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
  plotHeals: Partial<Record<PlotKey, PlotHealsStatus>>;
  removePlotHeals(plotKey: PlotKey): void;
  setPlotHeal(plotKey: PlotKey, status: boolean): void;
  isPlotHeal(plotKey: PlotKey): boolean;
};

export const plotHealsStore: StoreSlice<StatsHouseStore, PlotHealsStore> = (setState, getState) => ({
  plotHeals: {},
  removePlotHeals(plotKey) {
    setState((state) => {
      delete state.plotHeals[plotKey];
    });
  },
  setPlotHeal(plotKey, status) {
    setState((state) => {
      const heal = (state.plotHeals[plotKey] ??= getDefaultPlotHealsStatus());
      heal.response.push(status ? 1 : -1);
      heal.response = new Array(...heal.response.slice(-limitHistory));
      heal.lastTimestamp = Date.now();
      const sum = sumArray(heal.response);

      if (heal.status && sum < -changeStatus) {
        heal.status = false;
      }
      if (!heal.status && sum > changeStatus) {
        heal.status = true;
      }
      if (heal.status) {
        heal.timeout = 0;
      } else {
        if (status) {
          heal.timeout = Math.max(0, Math.floor((heal.timeout || 1) / 2));
        } else {
          heal.timeout = Math.min(maxTimeout, (heal.timeout || 1) * 2);
        }
      }
    });
  },
  isPlotHeal(plotKey) {
    const status = getState().plotHeals[plotKey];
    return !(!!status && !status.status && status.lastTimestamp + status.timeout * 1000 > Date.now());
  },
});

export function getDefaultPlotHealsStatus(): PlotHealsStatus {
  return {
    response: new Array(limitHistory).fill(1),
    status: true,
    lastTimestamp: Date.now(),
    timeout: 0,
  };
}
