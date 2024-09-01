import { type PlotKey } from 'url2';
import { useStatsHouse, viewPath } from 'store2';
import { useEffect } from 'react';
import { getAddPlotLink, getPlotLink, getPlotSingleLink } from 'store2/helpers';
import { To } from 'react-router-dom';
import { dequal } from 'dequal/lite';
import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';

type LinkPlot = {
  plotLinks: Partial<Record<PlotKey, To>>;
  singlePlotLinks: Partial<Record<PlotKey, To>>;
  addPlotLink?: To;
};

const useLinkPlots = create<LinkPlot>()(
  immer(() => ({
    plotLinks: {},
    singlePlotLinks: {},
  }))
);

useStatsHouse.subscribe((state, prevState) => {
  if (state.params !== prevState.params) {
    if (!dequal({ ...state.params, tabNum: '0' }, { ...prevState.params, tabNum: '0' })) {
      useLinkPlots.setState({ plotLinks: {}, singlePlotLinks: {}, addPlotLink: undefined }, true);
    }
  }
});

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
    if (!s.addPlotLink) {
      s.addPlotLink = getAddPlotLink(params, saveParams);
    }
  });
}

export function useLinkPlot(plotKey: PlotKey, visible?: boolean, single?: boolean): To {
  const visibleState = useStatsHouse(
    ({ plotPreviewList, plotVisibilityList }) => visible ?? (plotPreviewList[plotKey] || plotVisibilityList[plotKey])
  );
  const link = useLinkPlots((s) => (single ? s.singlePlotLinks[plotKey] : s.plotLinks[plotKey]) ?? '');

  useEffect(() => {
    if (visibleState && !link) {
      createPlotLink(plotKey, single);
    }
  }, [link, plotKey, single, visibleState]);
  return link;
}

export function useAddLinkPlot(visible: boolean): To {
  const link = useLinkPlots((s) => s.addPlotLink ?? '');
  useEffect(() => {
    if (visible && !link) {
      createAddPlotLink();
    }
  }, [link, visible]);
  return link;
}
