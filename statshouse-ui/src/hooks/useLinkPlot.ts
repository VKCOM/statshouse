import { PlotKey } from 'url2';
import { useStatsHouseShallow, viewPath } from 'store2';
import { useEffect, useMemo, useState } from 'react';
import { getPlotLink } from 'store2/helpers';
import { To } from 'react-router-dom';

export function useLinkPlot(plotKey: PlotKey, visible?: boolean): To {
  const { params, saveParams, visibleState } = useStatsHouseShallow(
    ({ params, saveParams, plotPreviewList, plotVisibilityList }) => ({
      params,
      saveParams,
      visibleState: visible ?? (plotPreviewList[plotKey] || plotVisibilityList[plotKey]),
    })
  );
  const [link, setLink] = useState<string>('');
  useEffect(() => {
    if (visibleState) {
      setLink(getPlotLink(plotKey, params, saveParams));
    }
  }, [params, plotKey, saveParams, visibleState]);
  return useMemo(
    () => ({
      pathname: viewPath[0],
      search: link,
    }),
    [link]
  );
}
