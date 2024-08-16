import { useStatsHouseShallow, viewPath } from 'store2';
import { useEffect, useMemo, useState } from 'react';
import { getAddPlotLink, getPlotLink } from 'store2/helpers';
import { To } from 'react-router-dom';

export function useAddLinkPlot(visible: boolean): To {
  const { params, saveParams } = useStatsHouseShallow(({ params, saveParams }) => ({ params, saveParams }));

  const [link, setLink] = useState<string>('');
  useEffect(() => {
    if (visible) {
      setLink(getAddPlotLink(params, saveParams));
    }
  }, [params, saveParams, visible]);
  return useMemo(
    () => ({
      pathname: viewPath[0],
      search: link,
    }),
    [link]
  );
}
