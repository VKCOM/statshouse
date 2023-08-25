import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import uPlot from 'uplot';
import { canvasToImageData } from '../../common/canvasToImage';
import { useStore } from '../statshouse';
import { Queue } from '../../common/Queue';
import { objectRemoveKeyShift, resortObjectKey } from '../../common/helpers';

const queuePreview = new Queue();

export type PlotPreview = {
  previewList: Record<string, string>;
  previewAbortController: Record<string, AbortController>;
};

export const usePlotPreview = create<PlotPreview>()(
  immer((setState) => ({
    previewList: {},
    previewAbortController: {},
  }))
);

export async function createPlotPreview(indexPlot: number, u: uPlot, width: number = 300) {
  const plotData = useStore.getState().plotsData[indexPlot];
  if (plotData?.data[0]?.length && plotData?.series.length) {
    usePlotPreview.getState().previewAbortController[indexPlot]?.abort();
    const controller = new AbortController();
    usePlotPreview.setState((state) => {
      state.previewAbortController[indexPlot] = controller;
    });
    try {
      const url = await queuePreview.add(
        () =>
          canvasToImageData(
            u.ctx.canvas,
            u.bbox.left,
            u.bbox.top,
            u.bbox.width,
            u.bbox.height,
            devicePixelRatio ? devicePixelRatio * width : width
          ),
        controller.signal
      );
      setPlotPreview(indexPlot, url);
    } catch (e) {
      // abort task
    }
    usePlotPreview.setState((state) => {
      delete state.previewAbortController[indexPlot];
    });
  }
}

export function setPlotPreview(indexPlot: number, url: string) {
  usePlotPreview.setState((state) => {
    const old = state.previewList[indexPlot];
    if (old && old.indexOf('blob:') === 0 && old !== url) {
      URL.revokeObjectURL(old);
    }
    state.previewList[indexPlot] = url;
  });
}

export function clearAllPlotPreview() {
  usePlotPreview.setState((state) => {
    state.previewList = {};
    state.previewAbortController = {};
  });
}

export function clearPlotPreview(indexPlot: number, remap?: boolean) {
  usePlotPreview.setState((state) => {
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
  usePlotPreview.setState((state) => {
    state.previewList = resortObjectKey(state.previewList, remap);
    state.previewAbortController = resortObjectKey(state.previewAbortController, remap);
  });
}
