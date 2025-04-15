// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { Queue } from '@/common/Queue';
import { createStore, StoreSlice } from '../createStore';
import { PlotKey } from '@/url2';
import { canvasToImageData } from '@/common/canvasToImage';
import { skipTimeout } from '@/common/helpers';
import { usePlotVisibilityStore } from '@/store2/plotVisibilityStore';

const queuePreview = new Queue();

export type PlotPreviewStore = {
  plotPreviewUrlList: Partial<Record<PlotKey, string>>;
  plotPreviewAbortController: Partial<Record<PlotKey, AbortController>>;
};
export const plotPreviewStore: StoreSlice<PlotPreviewStore, PlotPreviewStore> = () => ({
  plotPreviewUrlList: {},
  plotPreviewAbortController: {},
});

export const usePlotPreviewStore = createStore<PlotPreviewStore>(plotPreviewStore);

export async function createPlotPreview(plotKey: PlotKey, u: uPlot, width: number = 300) {
  await skipTimeout();
  if (
    !usePlotVisibilityStore.getState().plotPreviewList[plotKey] &&
    !usePlotVisibilityStore.getState().plotVisibilityList[plotKey]
  ) {
    return;
  }
  const controller = new AbortController();
  usePlotPreviewStore.setState((state) => {
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
    setPlotPreview(plotKey, url);
  } catch (_) {
    // abort task
  }
  usePlotPreviewStore.setState((state) => {
    delete state.plotPreviewAbortController[plotKey];
  });
}
export function setPlotPreview(plotKey: PlotKey, url: string) {
  usePlotPreviewStore.setState((state) => {
    const old = state.plotPreviewUrlList[plotKey];
    if (old && old.indexOf('blob:') === 0 && old !== url) {
      URL.revokeObjectURL(old);
    }
    state.plotPreviewUrlList[plotKey] = url;
  });
}

export function clearAllPlotPreview() {
  usePlotPreviewStore.setState((state) => {
    Object.values(state.plotPreviewUrlList).forEach((url) => {
      if (url) {
        URL.revokeObjectURL(url);
      }
    });
    state.plotPreviewUrlList = {};
    state.plotPreviewAbortController = {};
  });
}

export function clearPlotPreview(plotKey: PlotKey) {
  usePlotPreviewStore.setState((state) => {
    const url = state.plotPreviewUrlList[plotKey];
    if (url) {
      URL.revokeObjectURL(url);
    }
    delete state.plotPreviewUrlList[plotKey];
    delete state.plotPreviewAbortController[plotKey];
  });
}
