import { createStore } from '../createStore';
import { PlotKey, useUrlStore } from '../urlStore';
import { produce } from 'immer';

export type PlotVisibilityStore = {
  visibilityList: Partial<Record<string, boolean>>;
  previewList: Partial<Record<string, boolean>>;
};
export const usePlotVisibilityStore = createStore<PlotVisibilityStore>(
  () => ({
    visibilityList: {},
    previewList: {},
  }),
  'PlotVisibilityStore'
);

export function setPlotVisibility(plotKey: PlotKey, toggle: boolean) {
  toggle = useUrlStore.getState().params.tabNum === plotKey || toggle;
  if (usePlotVisibilityStore.getState().visibilityList[plotKey] !== toggle) {
    usePlotVisibilityStore.setState(
      produce((state) => {
        state.visibilityList[plotKey] = toggle;
      })
    );

    //todo:

    // if (toggle) {
    //   if (!useUrlStore.getState().numQueriesPlot[plotKey]) {
    //     useUrlStore.getState().loadPlot(plotKey);
    //   }
    //   useUrlStore.getState().params.plots[plotKey]?.events.forEach((iPlot) => {
    //     if (!useUrlStore.getState().numQueriesPlot[iPlot]) {
    //       useUrlStore.getState().loadPlot(iPlot);
    //     }
    //   });
    // }
  }
}

export function setPreviewVisibility(plotKey: PlotKey, toggle: boolean) {
  toggle = useUrlStore.getState().params.tabNum === plotKey || toggle;
  if (usePlotVisibilityStore.getState().previewList[plotKey] !== toggle) {
    usePlotVisibilityStore.setState(
      produce((state) => {
        state.previewList[plotKey] = toggle;
      })
    );

    //todo:

    // if (toggle && !useUrlStore.getState().numQueriesPlot[plotKey]) {
    //   useUrlStore.getState().loadPlot(plotKey);
    // }
  }
}

export function clearPlotVisibility(plotKey: PlotKey, remap?: boolean) {
  usePlotVisibilityStore.setState(
    produce((state) => {
      delete state.visibilityList[plotKey];
      delete state.previewList[plotKey];
    })
  );
}
