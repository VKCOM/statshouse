import { PlotKey } from '@/url2';
import { useStatsHouse } from '@/store2';
import { useCallback, useMemo } from 'react';

export function usePlotHeal(plotKey: PlotKey) {
  const status = useStatsHouse(
    useCallback(
      (s) => s.plotHeals[plotKey],
      // return !status || status.status || status.lastTimestamp + status.timeout * 1000 > Date.now();
      // return !(!!status && !status.status && status.lastTimestamp + status.timeout * 1000 > Date.now());
      [plotKey]
    )
  );
  return useMemo(() => !status || status.status || status.lastTimestamp + status.timeout * 1000, [status]);
}
