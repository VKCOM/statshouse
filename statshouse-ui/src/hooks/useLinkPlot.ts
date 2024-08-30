import { type PlotKey, type QueryParams } from 'url2';
import { useStatsHouseShallow, viewPath } from 'store2';
import { useEffect, useMemo, useRef, useState } from 'react';
import { getPlotLink } from 'store2/helpers';
import { To } from 'react-router-dom';
import { dequal } from 'dequal/lite';

export function useLinkPlot(plotKey: PlotKey, visible?: boolean): To {
  const { params, saveParams, visibleState } = useStatsHouseShallow(
    ({ params, saveParams, plotPreviewList, plotVisibilityList }) => ({
      params,
      saveParams,
      visibleState: visible ?? (plotPreviewList[plotKey] || plotVisibilityList[plotKey]),
    })
  );
  const oldParams = useRef<QueryParams | undefined>();
  const [link, setLink] = useState<string>('');
  useEffect(() => {
    if (visibleState) {
      if (!dequal({ ...oldParams.current, tabNum: '0' }, { ...params, tabNum: '0' })) {
        oldParams.current = params;
        setLink(getPlotLink(plotKey, params, saveParams));
      }
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
