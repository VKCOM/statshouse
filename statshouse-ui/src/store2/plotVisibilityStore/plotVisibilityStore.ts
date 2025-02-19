// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { createStore, StoreSlice } from '../createStore';
import { PlotKey } from '@/url2';

export type PlotVisibilityStore = {
  plotVisibilityList: Partial<Record<PlotKey, boolean>>;
  plotPreviewList: Partial<Record<PlotKey, boolean>>;
};

export const plotVisibilityStore: StoreSlice<PlotVisibilityStore, PlotVisibilityStore> = () => ({
  plotVisibilityList: {},
  plotPreviewList: {},
});
export const usePlotVisibilityStore = createStore<PlotVisibilityStore>(plotVisibilityStore);

export function setPlotVisibility(plotKey: PlotKey, toggle: boolean) {
  usePlotVisibilityStore.setState((state) => {
    state.plotVisibilityList[plotKey] = toggle;
  });
}

export function setPlotPreviewVisibility(plotKey: PlotKey, toggle: boolean) {
  usePlotVisibilityStore.setState((state) => {
    state.plotPreviewList[plotKey] = toggle;
  });
}

export function clearPlotVisibility(plotKey: PlotKey) {
  usePlotVisibilityStore.setState((state) => {
    delete state.plotVisibilityList[plotKey];
    delete state.plotPreviewList[plotKey];
  });
}
