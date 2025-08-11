import { useSearchParams } from 'react-router-dom';
import { useMemo } from 'react';
import { GET_PARAMS } from '@/api/enum';

export function useHistoricalMetricVersion() {
  const [searchParams] = useSearchParams();
  return useMemo(() => searchParams.get(GET_PARAMS.metricUrlVersion), [searchParams]);
}
