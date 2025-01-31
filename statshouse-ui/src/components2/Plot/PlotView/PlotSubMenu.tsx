// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo, useCallback } from 'react';
import { PlotKey } from '@/url2';
import { Link } from 'react-router-dom';
import cn from 'classnames';
import { useStatsHouseShallow } from '@/store2';
import { formatSI } from '@/common/formatByMetricType';
import { useLinkCSV2 } from '@/hooks/useLinkCSV2';
import { useMetricBadges } from '@/hooks/useMetricBadges';
import { isPromQL } from '@/store2/helpers';

export type PlotSubMenuProps = {
  className?: string;
  plotKey: PlotKey;
};

export const PlotSubMenu = memo(function PlotSubMenu({ className, plotKey }: PlotSubMenuProps) {
  const { metricName, params, plot, timeRange } = useStatsHouseShallow(
    useCallback(
      ({ plotsData, params }) => ({
        metricName:
          plotsData[plotKey]?.metricName ??
          (isPromQL(params.plots[plotKey]) ? '' : params.plots[plotKey]?.metricName) ??
          '',
        params,
        plot: params.plots[plotKey],
        timeRange: params.timeRange,
      }),
      [plotKey]
    )
  );
  const { receiveErrors, receiveWarnings, samplingFactorSrc, samplingFactorAgg, mappingFloodEvents, isLoading } =
    useMetricBadges(plot, params);

  const linkCSV = useLinkCSV2(plotKey);
  return (
    <ul className={cn('nav', className)}>
      <li className="nav-item">
        {receiveErrors > 0.5 || receiveWarnings > 0.5 ? (
          <Link
            className="nav-link p-0 me-4"
            target="_blank"
            to={{
              search: `?s=__src_ingestion_status&f=${timeRange.from}&t=${timeRange.urlTo}&qf=key1-${metricName}&qb=key2&qf=key2~ok_cached&qf=key2~ok_uncached`,
            }}
          >
            {receiveErrors > 0.5 ? (
              <small className="badge bg-danger">Receive errors: {formatSI(receiveErrors)}</small>
            ) : (
              <small className="badge bg-warning text-dark">Receive warnings: {formatSI(receiveWarnings)}</small>
            )}
          </Link>
        ) : (
          <Link
            className="nav-link p-0 me-4"
            target="_blank"
            to={{
              search: `?s=__src_ingestion_status&f=${timeRange.from}&t=${timeRange.urlTo}&qf=key1-${metricName}&qb=key2`,
            }}
          >
            <small>
              Receive status {isLoading && <span className="spinner-border spinner-border-sm-09" role="status"></span>}
            </small>
          </Link>
        )}
      </li>
      <li className="nav-item text-muted">
        <small className="me-4">
          Sampling:{' '}
          <Link
            className="nav-link d-inline-block p-0"
            target="_blank"
            to={{
              search: `?s=__src_sampling_factor&qw=avg&f=${timeRange.from}&t=${timeRange.urlTo}&qf=key1-${metricName}`,
            }}
          >
            {samplingFactorSrc > 5 ? (
              <span className="badge bg-danger">source (&gt;5)</span>
            ) : samplingFactorSrc > 1.02 ? (
              <span className="badge bg-warning text-dark">source</span>
            ) : (
              <span>
                source {isLoading && <span className="spinner-border spinner-border-sm-09" role="status"></span>}
              </span>
            )}
          </Link>{' '}
          /{' '}
          <Link
            className="nav-link d-inline-block p-0"
            target="_blank"
            to={{
              search: `?s=__agg_sampling_factor&qw=avg&f=${timeRange.from}&t=${timeRange.urlTo}&qf=key4-${metricName}`,
            }}
          >
            {samplingFactorAgg > 5 ? (
              <span className="badge bg-danger">aggregator (&gt;5)</span>
            ) : samplingFactorAgg > 1.02 ? (
              <span className="badge bg-warning text-dark">aggregator</span>
            ) : (
              <span>
                aggregator {isLoading && <span className="spinner-border spinner-border-sm-09" role="status"></span>}
              </span>
            )}
          </Link>
        </small>
      </li>
      <li className="nav-item">
        <Link
          className="nav-link p-0 me-4"
          target="_blank"
          to={{
            search: `?s=__agg_hour_cardinality&qw=sum_norm&f=${timeRange.from}&t=${timeRange.urlTo}&qf=key4-${metricName}`,
          }}
        >
          <small>Cardinality</small>
        </Link>
      </li>
      <li className="nav-item">
        {mappingFloodEvents > 0.5 ? (
          <Link
            className="nav-link p-0 me-4"
            target="_blank"
            to={{
              search: `?s=__agg_mapping_created&qw=count&f=${timeRange.from}&t=${timeRange.urlTo}&qf=key4-${metricName}&qb=key5&qf=key5~created`,
            }}
          >
            <small className="badge bg-danger">Mapping errors: {formatSI(mappingFloodEvents)}</small>
          </Link>
        ) : (
          <Link
            className="nav-link p-0 me-4"
            target="_blank"
            to={{
              search: `?s=__agg_mapping_created&qw=count&f=${timeRange.from}&t=${timeRange.urlTo}&qf=key4-${metricName}&qb=key5`,
            }}
          >
            <small>Mapping status</small>
          </Link>
        )}
      </li>
      <li className="nav-item">
        <Link className="nav-link p-0 me-4" to={linkCSV} download target="_blank">
          <small>CSV</small>
        </Link>
      </li>
      <li className="nav-item">
        <Link className="nav-link p-0" to={`../admin/edit/${metricName}`}>
          <small>Edit</small>
        </Link>
      </li>
    </ul>
  );
});
