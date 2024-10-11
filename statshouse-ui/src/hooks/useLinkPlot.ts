import { type PlotKey } from 'url2';
import { useStatsHouse, useStatsHouseShallow, viewPath } from 'store2';
import { useEffect, useMemo } from 'react';
import { getAddPlotLink, getPlotLink, getPlotSingleLink } from 'store2/helpers';
import { To } from 'react-router-dom';
import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import { usePlotVisibilityStore } from 'store2/plotVisibilityStore';
import { getUrlObject } from '../common/getUrlObject';

type LinkPlot = {
  plotLinks: Partial<Record<PlotKey, To>>;
  singlePlotLinks: Partial<Record<PlotKey, To>>;
  addPlotLink?: { pathname?: string; hash?: string; search?: string };
};

export const useLinkPlots = create<LinkPlot>()(
  immer(() => ({
    plotLinks: {},
    singlePlotLinks: {},
  }))
);

export function createPlotLink(plotKey: PlotKey, single?: boolean) {
  const { params, saveParams } = useStatsHouse.getState();
  useLinkPlots.setState((s) => {
    if (single) {
      if (!s.singlePlotLinks[plotKey]) {
        s.singlePlotLinks[plotKey] = {
          pathname: viewPath[0],
          ...getUrlObject(getPlotSingleLink(plotKey, params)),
        };
      }
    } else {
      if (!s.plotLinks[plotKey]) {
        s.plotLinks[plotKey] = {
          pathname: viewPath[0],
          ...getUrlObject(getPlotLink(plotKey, params, saveParams)),
        };
      }
    }
  });
}

export function createAddPlotLink() {
  const { params, saveParams } = useStatsHouse.getState();
  const nextUrl = getUrlObject(getAddPlotLink(params, saveParams));
  useLinkPlots.setState((s) => {
    if (!s.addPlotLink || (nextUrl.hash !== s.addPlotLink?.hash && nextUrl.search !== s.addPlotLink?.search)) {
      s.addPlotLink = {
        pathname: viewPath[0],
        ...nextUrl,
      };
    }
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
  const { params, saveParams } = useStatsHouseShallow(({ params, saveParams }) => ({ params, saveParams }));
  return useMemo<To>(
    () => ({
      pathname: viewPath[0],
      ...getUrlObject(getAddPlotLink(params, saveParams)),
    }),
    [params, saveParams]
  );
}
