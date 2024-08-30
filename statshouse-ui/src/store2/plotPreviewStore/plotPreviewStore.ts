// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { Queue } from 'common/Queue';
import { StoreSlice } from '../createStore';
import { StatsHouseStore, useStatsHouse } from '../statsHouseStore';
import { PlotKey } from 'url2';
import { canvasToImageData } from 'common/canvasToImage';

const queuePreview = new Queue();

export type PlotPreviewStore = {
  plotPreviewUrlList: Partial<Record<PlotKey, string>>;
  plotPreviewAbortController: Partial<Record<PlotKey, AbortController>>;
  createPlotPreview(plotKey: PlotKey, u: uPlot, width?: number): Promise<void>;
  clearAllPlotPreview(): void;
  setPlotPreview(plotKey: PlotKey, url: string): void;
};
export const plotPreviewStore: StoreSlice<StatsHouseStore, PlotPreviewStore> = (setState, getState, store) => {
  store.subscribe((state, prevState) => {
    if (state.plotsData !== prevState.plotsData) {
      //todo:
    }
  });
  return {
    plotPreviewUrlList: {},
    plotPreviewAbortController: {},
    async createPlotPreview(plotKey, u, width = 300) {
      if (!getState().plotPreviewList[plotKey]) {
        return;
      }
      const controller = new AbortController();
      setState((state) => {
        state.plotPreviewAbortController[plotKey]?.abort();
        state.plotPreviewAbortController[plotKey] = controller;
      });
      try {
        const canvas = u.ctx.canvas;
        const canvasLeft = u.bbox.left;
        const canvasTop = u.bbox.top;
        const canvasWidth = u.bbox.width;
        const canvasHeight = u.bbox.height;
        const url = await queuePreview.add(
          () =>
            canvasToImageData(
              canvas,
              canvasLeft,
              canvasTop,
              canvasWidth,
              canvasHeight,
              devicePixelRatio ? devicePixelRatio * width : width
            ),
          controller.signal
        );
        getState().setPlotPreview(plotKey, url);
      } catch (e) {
        // abort task
      }
      setState((state) => {
        delete state.plotPreviewAbortController[plotKey];
      });
    },
    setPlotPreview(plotKey, url) {
      setState((state) => {
        const old = state.plotPreviewUrlList[plotKey];
        if (old && old.indexOf('blob:') === 0 && old !== url) {
          URL.revokeObjectURL(old);
        }
        state.plotPreviewUrlList[plotKey] = url;
      });
    },
    clearAllPlotPreview() {
      setState((state) => {
        Object.values(state.plotPreviewUrlList).forEach((url) => {
          url && URL.revokeObjectURL(url);
        });
        state.plotPreviewUrlList = {};
        state.plotPreviewAbortController = {};
      });
    },
    clearPlotPreview(plotKey: PlotKey) {
      setState((state) => {
        const url = state.plotPreviewUrlList[plotKey];
        if (url) {
          URL.revokeObjectURL(url);
        }
        delete state.plotPreviewUrlList[plotKey];
        delete state.plotPreviewAbortController[plotKey];
      });
    },
  };
};
