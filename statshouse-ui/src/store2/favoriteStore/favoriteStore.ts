import { createJSONStorage, persist, type StateStorage } from 'zustand/middleware';
import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import { isNotNil, isObject } from 'common/helpers';

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
  metricsFavorite: Partial<Record<number, boolean>>;
};

export const useFavoriteStore = create<FavoriteStore>()(
  persist(
    immer(() => ({ dashboardsFavorite: {}, metricsFavorite: {} })),
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
          }
          return value;
        },
        replacer: (key, value) => {
          switch (key) {
            case 'dashboardsFavorite':
            case 'metricsFavorite':
              if (isObject(value)) {
                return Object.keys(value).map(Number).filter(isNotNil);
              }
              return [];
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

export function toggleMetricsFavorite(id: number) {
  useFavoriteStore.setState((s) => {
    if (s.metricsFavorite[id]) {
      delete s.metricsFavorite[id];
    } else {
      s.metricsFavorite[id] = true;
    }
  });
}
