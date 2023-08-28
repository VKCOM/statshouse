import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import { useStore } from '../statshouse';
import { objectRemoveKeyShift, resortObjectKey } from '../../common/helpers';

export type PlotVisibilityStore = {
  visibilityList: Record<string, boolean>;
  previewList: Record<string, boolean>;
};
export const usePlotVisibilityStore = create<PlotVisibilityStore>()(
  immer((setState) => ({
    visibilityList: {},
    previewList: {},
  }))
);

export function setPlotVisibility(indexPlot: number, toggle: boolean) {
  toggle = useStore.getState().params.tabNum === indexPlot || toggle;
  if (usePlotVisibilityStore.getState().visibilityList[indexPlot] !== toggle) {
    usePlotVisibilityStore.setState((state) => {
      state.visibilityList[indexPlot] = toggle;
    });
    if (toggle && !useStore.getState().numQueriesPlot[indexPlot]) {
      useStore.getState().loadPlot(indexPlot);
    }
  }
}

export function setPreviewVisibility(indexPlot: number, toggle: boolean) {
  toggle = useStore.getState().params.tabNum === indexPlot || toggle;
  if (usePlotVisibilityStore.getState().previewList[indexPlot] !== toggle) {
    usePlotVisibilityStore.setState((state) => {
      state.previewList[indexPlot] = toggle;
    });
    if (toggle && !useStore.getState().numQueriesPlot[indexPlot]) {
      useStore.getState().loadPlot(indexPlot);
    }
  }
}

export function clearPlotVisibility(indexPlot: number, remap?: boolean) {
  usePlotVisibilityStore.setState((state) => {
    delete state.visibilityList[indexPlot];
    delete state.previewList[indexPlot];
    if (remap) {
      state.visibilityList = objectRemoveKeyShift(state.visibilityList, indexPlot);
      state.previewList = objectRemoveKeyShift(state.previewList, indexPlot);
    }
  });
}

export function resortPlotVisibility(remap: Record<string, string | number>) {
  usePlotVisibilityStore.setState((state) => {
    state.visibilityList = resortObjectKey(state.visibilityList, remap);
    state.previewList = resortObjectKey(state.previewList, remap);
  });
}
