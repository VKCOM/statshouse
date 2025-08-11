// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { NavLink, useParams } from 'react-router-dom';
import { HistoryDashboardLabel } from '@/components2/HistoryDashboardLabel';
import { StickyTop } from '@/components2/StickyTop';
import { useApiMetric } from '@/api/metric';
import css from './style.module.css';
import cn from 'classnames';
import { useHistoricalMetricVersion } from '@/hooks/useHistoricalMetricVersion';
import { GET_PARAMS } from '@/api/enum';
import { useMemo } from 'react';

export type MetricEditMenuProps = {};

export function MetricEditMenu({}: MetricEditMenuProps) {
  const { metricName } = useParams();
  const historicalMetricVersion = useHistoricalMetricVersion();
  const queryMetric = useApiMetric(metricName ?? '', historicalMetricVersion);
  const metric = queryMetric.data?.data.metric;
  const isHistoricalMetric = !!metric && metric.version !== metric.currentVersion;
  const search = useMemo(
    () =>
      historicalMetricVersion
        ? new URLSearchParams([[GET_PARAMS.metricUrlVersion, historicalMetricVersion.toString()]]).toString()
        : undefined,
    [historicalMetricVersion]
  );
  if (queryMetric.isError) {
    return null;
  }
  return (
    <StickyTop className="mb-3">
      <div className="d-flex">
        <div className="my-auto">
          <h6 className="overflow-force-wrap font-monospace fw-bold me-3 my-auto" title={`ID: ${metric?.id || '?'}`}>
            {metricName}:
            <NavLink
              to={{
                pathname: '',
                search,
              }}
              end
              className={cn('mx-4', css.link)}
            >
              edit
            </NavLink>
            <NavLink
              to={{
                pathname: 'history',
                search,
              }}
              className={cn('me-4', css.link)}
            >
              history
            </NavLink>
            <NavLink className={cn('me-4', css.link)} to={`/view?s=${metricName}`}>
              view
            </NavLink>
          </h6>
        </div>

        {isHistoricalMetric && <HistoryDashboardLabel />}
      </div>
    </StickyTop>
  );
}
