// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { HistoryList } from '@/components2/HistoryList';
import { useParams } from 'react-router-dom';
import { useApiMetric } from '@/api/metric';
import { useEffect, useMemo } from 'react';
import { GET_PARAMS } from '@/api/enum';
import { useHistoricalMetricVersion } from '@/hooks/useHistoricalMetricVersion';

export type MetricHistoryProps = { adminMode?: boolean };

export function MetricHistory({}: MetricHistoryProps) {
  const { metricName } = useParams();
  const historicalMetricVersion = useHistoricalMetricVersion();
  const queryMetric = useApiMetric(metricName ?? '', historicalMetricVersion);
  const error = queryMetric.error;
  const metric = queryMetric.data?.data.metric;
  const mainPath = useMemo(() => `/admin/edit/${metricName}`, [metricName]);

  // update document title
  useEffect(() => {
    document.title = `${metricName + ': history'} — StatsHouse`;
  }, [metricName]);
  if (error) {
    return (
      <div className="alert alert-danger" role="alert">
        {error.message}
      </div>
    );
  }
  if (!metric?.metric_id) {
    return (
      <div className="d-flex justify-content-center align-items-center mt-5">
        <div className="spinner-border text-secondary" role="status">
          <span className="visually-hidden">Loading...</span>
        </div>
      </div>
    );
  }
  return (
    <HistoryList
      id={metric?.metric_id.toString()}
      mainPath={mainPath}
      pathVersionParam={'?' + GET_PARAMS.metricUrlVersion}
    />
  );
}
