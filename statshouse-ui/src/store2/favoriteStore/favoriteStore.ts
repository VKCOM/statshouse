// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { createJSONStorage, persist, type StateStorage } from 'zustand/middleware';
import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import { isNotNil, isObject } from '@/common/helpers';

export const favoriteStateStorage: StateStorage = {
  getItem(name) {
    try {
      return localStorage.getItem(name);
    } catch (_) {
      return null;
    }
  },
  setItem(name, value) {
    localStorage.setItem(name, value);
  },
  removeItem(name: string) {
    localStorage.removeItem(name);
  },
};

export type FavoriteStore = {
  dashboardsFavorite: Partial<Record<number, boolean>>;
  metricsFavorite: Partial<Record<string, boolean>>;
  showMetricsFavorite: boolean;
};

export const useFavoriteStore = create<FavoriteStore>()(
  persist(
    immer(() => ({ dashboardsFavorite: {}, metricsFavorite: {}, showMetricsFavorite: false }) as FavoriteStore),
    {
      name: 'sh-favorite',
      version: 0,
      storage: createJSONStorage(() => favoriteStateStorage, {
        reviver: (key, value) => {
          switch (key) {
            case 'dashboardsFavorite':
            case 'metricsFavorite':
              if (Array.isArray(value)) {
                return value.reduce((res, v) => {
                  res[v] = true;
                  return res;
                }, {});
              }
              return {};
            // case 'showMetricsFavorite':
            //   return false;
          }
          return value;
        },
        replacer: (key, value) => {
          switch (key) {
            case 'dashboardsFavorite':
              if (isObject(value)) {
                return Object.keys(value).map(Number).filter(isNotNil);
              }
              return [];
            case 'metricsFavorite':
              if (isObject(value)) {
                return Object.keys(value);
              }
              return [];
            // case 'showMetricsFavorite':
            //   return 0;
          }
          return value;
        },
      }),
    }
  )
);

export function toggleDashboardsFavorite(id: number) {
  useFavoriteStore.setState((s) => {
    if (s.dashboardsFavorite[id]) {
      delete s.dashboardsFavorite[id];
    } else {
      s.dashboardsFavorite[id] = true;
    }
  });
}

export function toggleMetricsFavorite(name: string) {
  useFavoriteStore.setState((s) => {
    if (s.metricsFavorite[name]) {
      delete s.metricsFavorite[name];
    } else {
      s.metricsFavorite[name] = true;
    }
  });
}

export function toggleShowMetricsFavorite(status?: boolean) {
  useFavoriteStore.setState((s) => {
    s.showMetricsFavorite = status ?? !s.showMetricsFavorite;
  });
}
