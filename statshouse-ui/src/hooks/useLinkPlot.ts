import { type PlotKey } from 'url2';
import { useStatsHouse, viewPath } from 'store2';
import { useEffect } from 'react';
import { getAddPlotLink, getPlotLink, getPlotSingleLink } from 'store2/helpers';
import { To } from 'react-router-dom';
import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import { usePlotVisibilityStore } from 'store2/plotVisibilityStore';

type LinkPlot = {
  plotLinks: Partial<Record<PlotKey, To>>;
  singlePlotLinks: Partial<Record<PlotKey, To>>;
  addPlotLink?: To;
};

export const useLinkPlots = create<LinkPlot>()(
  immer(() => ({
    plotLinks: {},
    singlePlotLinks: {},
  }))
);

function createPlotLink(plotKey: PlotKey, single?: boolean) {
  const { params, saveParams } = useStatsHouse.getState();
  useLinkPlots.setState((s) => {
    if (single) {
      if (!s.singlePlotLinks[plotKey]) {
        s.singlePlotLinks[plotKey] = {
          pathname: viewPath[0],
          search: getPlotSingleLink(plotKey, params),
        };
      }
    } else {
      if (!s.plotLinks[plotKey]) {
        s.plotLinks[plotKey] = {
          pathname: viewPath[0],
          search: getPlotLink(plotKey, params, saveParams),
        };
      }
    }
  });
}

function createAddPlotLink() {
  const { params, saveParams } = useStatsHouse.getState();
  useLinkPlots.setState((s) => {
    s.addPlotLink = getAddPlotLink(params, saveParams);
  });
}

export function useLinkPlot(plotKey: PlotKey, visible?: boolean, single?: boolean): To {
  const visibleState = usePlotVisibilityStore(
    ({ plotPreviewList, plotVisibilityList }) =>
      visible ?? ((plotPreviewList[plotKey] && !single) || plotVisibilityList[plotKey])
  );
  const link = useLinkPlots((s) => (single ? s.singlePlotLinks[plotKey] : s.plotLinks[plotKey]) ?? '');

  useEffect(() => {
    if (visibleState && !link) {
      createPlotLink(plotKey, single);
    }
  }, [link, plotKey, single, visible, visibleState]);
  return link;
}

export function useAddLinkPlot(visible: boolean): To {
  const tabNum = useStatsHouse((s) => s.params.tabNum);
  const link = useLinkPlots((s) => s.addPlotLink ?? '');
  useEffect(() => {
    if (visible) {
      createAddPlotLink();
    }
  }, [visible, tabNum]);
  return link;
}
