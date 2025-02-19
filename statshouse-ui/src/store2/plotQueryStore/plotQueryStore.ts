// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { createStore } from '../createStore';
import type { PlotKey } from '@/url2';
import { useCallback } from 'react';

export type PlotQueryStore = {
  globalQuery: number;
  plotQuery: Partial<Record<PlotKey, number>>;
};

export const usePlotQueryStore = createStore<PlotQueryStore>(() => ({
  globalQuery: 0,
  plotQuery: {},
}));

export function queryStart(plotKey: PlotKey) {
  usePlotQueryStore.setState((state) => {
    const q = state.plotQuery[plotKey] ?? 0;
    state.plotQuery[plotKey] = q + 1;
  });
  let start = true;
  return () => {
    if (start) {
      start = false;
      usePlotQueryStore.setState((state) => {
        const q = state.plotQuery[plotKey];
        if (q != null) {
          state.plotQuery[plotKey] = q - 1;
        }
      });
    }
  };
}

export function globalQueryStart() {
  usePlotQueryStore.setState((state) => {
    state.globalQuery++;
  });
  let start = true;
  return () => {
    if (start) {
      start = false;
      usePlotQueryStore.setState((state) => {
        state.globalQuery--;
      });
    }
  };
}

export function getPlotLoader(plotKey: PlotKey) {
  return (usePlotQueryStore.getState().plotQuery[plotKey] ?? 0) > 0;
}
export function getGlobalLoader() {
  return usePlotQueryStore.getState().globalQuery > 0;
}

export function usePlotLoader(plotKey: PlotKey) {
  return usePlotQueryStore(useCallback((s) => (s.plotQuery[plotKey] ?? 0) > 0, [plotKey]));
}

export function useGlobalLoader() {
  return usePlotQueryStore((s) => s.globalQuery > 0);
}
