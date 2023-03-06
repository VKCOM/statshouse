import { themeState, type ThemeStore } from './theme/themeStore';
import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import { statsHouseState, type StatsHouseStore } from './statshouse';

export type { StatsHouseStore, PlotStore, PlotValues, TopInfo } from './statshouse';
export type { ThemeStore, Theme } from './theme/themeStore';
export { THEMES } from './theme/themeStore';

export type Store = StatsHouseStore & ThemeStore;
export const useStore = create<Store, [['zustand/immer', never]]>(
  immer((...a) => ({
    ...statsHouseState(...a),
    ...themeState(...a),
  }))
);
