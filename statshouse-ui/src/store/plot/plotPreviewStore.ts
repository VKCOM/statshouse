import uPlot from 'uplot';
import { canvasToImageData } from '../../common/canvasToImage';
import { Queue } from '../../common/Queue';
import { objectRemoveKeyShift, resortObjectKey } from '../../common/helpers';
import { createStore } from '../createStore';

const queuePreview = new Queue();

export type PlotPreviewStore = {
  previewList: Record<string, string>;
  previewAbortController: Record<string, AbortController>;
};

export const usePlotPreviewStore = createStore<PlotPreviewStore>(
  () => ({
    previewList: {},
    previewAbortController: {},
  }),
  'PlotPreview'
);

export async function createPlotPreview(indexPlot: number, u: uPlot, width: number = 300) {
  const controller = new AbortController();
  usePlotPreviewStore.setState((state) => {
    state.previewAbortController[indexPlot]?.abort();
    state.previewAbortController[indexPlot] = controller;
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
    setPlotPreview(indexPlot, url);
  } catch (e) {
    // abort task
  }
  usePlotPreviewStore.setState((state) => {
    delete state.previewAbortController[indexPlot];
  });
  // }
}

export function setPlotPreview(indexPlot: number, url: string) {
  usePlotPreviewStore.setState((state) => {
    const old = state.previewList[indexPlot];
    if (old && old.indexOf('blob:') === 0 && old !== url) {
      URL.revokeObjectURL(old);
    }
    state.previewList[indexPlot] = url;
  });
}

export function clearAllPlotPreview() {
  usePlotPreviewStore.setState((state) => {
    state.previewList = {};
    state.previewAbortController = {};
  });
}

export function clearPlotPreview(indexPlot: number, remap?: boolean) {
  usePlotPreviewStore.setState((state) => {
    if (state.previewList[indexPlot]) {
      URL.revokeObjectURL(state.previewList[indexPlot]);
    }
    delete state.previewList[indexPlot];
    delete state.previewAbortController[indexPlot];
    if (remap) {
      state.previewList = objectRemoveKeyShift(state.previewList, indexPlot);
      state.previewAbortController = objectRemoveKeyShift(state.previewAbortController, indexPlot);
    }
  });
}

export function resortPlotPreview(remap: Record<string, string | number>) {
  usePlotPreviewStore.setState((state) => {
    state.previewList = resortObjectKey(state.previewList, remap);
    state.previewAbortController = resortObjectKey(state.previewAbortController, remap);
  });
}
