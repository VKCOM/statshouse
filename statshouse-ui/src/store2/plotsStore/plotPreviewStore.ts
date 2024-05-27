import uPlot from 'uplot';
import { canvasToImageData } from '../../common/canvasToImage';
import { Queue } from '../../common/Queue';
import { createStore } from '../createStore';
import { produce } from 'immer';
import { PlotKey } from '../urlStore';

const queuePreview = new Queue();

export type PlotPreviewStore = {
  previewList: Partial<Record<string, string>>;
  previewAbortController: Partial<Record<string, AbortController>>;
};

export const usePlotPreviewStore = createStore<PlotPreviewStore>(
  () => ({
    previewList: {},
    previewAbortController: {},
  }),
  'PlotPreview'
);

export async function createPlotPreview(plotKey: PlotKey, u: uPlot, width: number = 300) {
  const controller = new AbortController();
  usePlotPreviewStore.setState(
    produce((state) => {
      state.previewAbortController[plotKey]?.abort();
      state.previewAbortController[plotKey] = controller;
    })
  );
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
  } catch (e) {
    // abort task
  }
  usePlotPreviewStore.setState(
    produce((state) => {
      delete state.previewAbortController[plotKey];
    })
  );
}

export function setPlotPreview(plotKey: PlotKey, url: string) {
  usePlotPreviewStore.setState(
    produce((state) => {
      const old = state.previewList[plotKey];
      if (old && old.indexOf('blob:') === 0 && old !== url) {
        URL.revokeObjectURL(old);
      }
      state.previewList[plotKey] = url;
    })
  );
}

export function clearAllPlotPreview() {
  usePlotPreviewStore.setState(
    produce((state) => {
      state.previewList = {};
      state.previewAbortController = {};
    })
  );
}

export function clearPlotPreview(plotKey: PlotKey, remap?: boolean) {
  usePlotPreviewStore.setState(
    produce((state) => {
      if (state.previewList[plotKey]) {
        URL.revokeObjectURL(state.previewList[plotKey]);
      }
      delete state.previewList[plotKey];
      delete state.previewAbortController[plotKey];
    })
  );
}
