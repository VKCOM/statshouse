import { useSearchParams } from 'react-router-dom';
import { useMemo } from 'react';
import { GET_PARAMS } from '@/api/enum';
import { toNumber } from '@/common/helpers';

export function useHistoricalMetricVersion() {
  const [searchParams] = useSearchParams();
  return useMemo(() => toNumber(searchParams.get(GET_PARAMS.metricUrlVersion)), [searchParams]);
}
