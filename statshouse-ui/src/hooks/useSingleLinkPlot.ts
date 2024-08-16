import { PlotKey } from 'url2';
import { useStatsHouse, useStatsHouseShallow, viewPath } from 'store2';
import { useEffect, useMemo, useState } from 'react';
import { getPlotLink } from 'store2/helpers';
import { To } from 'react-router-dom';

export function useSingleLinkPlot(plotKey: PlotKey, visible?: boolean): To {
  const { params, visibleState } = useStatsHouseShallow(({ params, plotPreviewList, plotVisibilityList }) => ({
    params,
    visibleState: visible ?? (plotPreviewList[plotKey] || plotVisibilityList[plotKey]),
  }));
  const [link, setLink] = useState<string>('');
  useEffect(() => {
    if (visibleState) {
      setLink(getPlotLink(plotKey, params));
    }
  }, [params, plotKey, visibleState]);
  return useMemo(
    () => ({
      pathname: viewPath[0],
      search: link,
    }),
    [link]
  );
}
