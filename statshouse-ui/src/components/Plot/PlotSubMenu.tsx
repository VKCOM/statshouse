// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo } from 'react';
import * as utils from '../../view/utils';
import { TimeRange } from '../../common/TimeRange';
import { Link } from 'react-router-dom';
import { querySelector } from '../../view/api';

export type PlotSubMenuProps = {
  sel: querySelector;
  timeRange: TimeRange;
  receiveErrors: number;
  samplingFactorSrc: number;
  samplingFactorAgg: number;
  mappingFloodEvents: number;
  linkCSV: string;
};
export const _PlotSubMenu: React.FC<PlotSubMenuProps> = ({
  timeRange,
  sel,
  receiveErrors,
  samplingFactorSrc,
  samplingFactorAgg,
  mappingFloodEvents,
  linkCSV,
}) => (
  <ul className="nav">
    <li className="nav-item">
      {receiveErrors > 0.5 ? (
        <Link
          className="nav-link p-0 me-4"
          to={{
            search: `?s=__src_ingestion_status&f=${timeRange.relativeFrom}&t=${timeRange.to}&qf=key1-${sel.metricName}&qb=key2&qf=key2~ok_cached&qf=key2~ok_uncached`,
          }}
        >
          <small className="badge bg-danger">Receive errors: {utils.formatSI(receiveErrors)}</small>
        </Link>
      ) : (
        <Link
          className="nav-link p-0 me-4"
          to={{
            search: `?s=__src_ingestion_status&f=${timeRange.relativeFrom}&t=${timeRange.to}&qf=key1-${sel.metricName}&qb=key2`,
          }}
        >
          <small>Receive status</small>
        </Link>
      )}
    </li>
    <li className="nav-item text-muted">
      <small className="me-4">
        Sampling:{' '}
        <Link
          className="nav-link d-inline-block p-0"
          to={{
            search: `?s=__src_sampling_factor&qw=avg&f=${timeRange.relativeFrom}&t=${timeRange.to}&qf=key1-${sel.metricName}`,
          }}
        >
          {samplingFactorSrc > 5 ? (
            <span className="badge bg-danger">source (&gt;5)</span>
          ) : samplingFactorSrc > 1.02 ? (
            <span className="badge bg-warning text-dark">source</span>
          ) : (
            <span>source</span>
          )}
        </Link>{' '}
        /{' '}
        <Link
          className="nav-link d-inline-block p-0"
          to={{
            search: `?s=__agg_sampling_factor&qw=avg&f=${timeRange.relativeFrom}&t=${timeRange.to}&qf=key4-${sel.metricName}`,
          }}
        >
          {samplingFactorAgg > 5 ? (
            <span className="badge bg-danger">aggregator (&gt;5)</span>
          ) : samplingFactorAgg > 1.02 ? (
            <span className="badge bg-warning text-dark">aggregator</span>
          ) : (
            <span>aggregator</span>
          )}
        </Link>
      </small>
    </li>
    <li className="nav-item">
      <Link
        className="nav-link p-0 me-4"
        to={{
          search: `?s=__agg_hour_cardinality&qw=sum_norm&f=${timeRange.relativeFrom}&t=${timeRange.to}&qf=key4-${sel.metricName}`,
        }}
      >
        <small>Cardinality</small>
      </Link>
    </li>
    <li className="nav-item">
      {mappingFloodEvents > 0.5 ? (
        <Link
          className="nav-link p-0 me-4"
          to={{
            search: `?s=__agg_mapping_created&qw=count&f=${timeRange.relativeFrom}&t=${timeRange.to}&qf=key4-${sel.metricName}&qb=key5&qf=key5~created`,
          }}
        >
          <small className="badge bg-danger">Mapping errors: {utils.formatSI(mappingFloodEvents)}</small>
        </Link>
      ) : (
        <Link
          className="nav-link p-0 me-4"
          to={{
            search: `?s=__agg_mapping_created&qw=count&f=${timeRange.relativeFrom}&t=${timeRange.to}&qf=key4-${sel.metricName}&qb=key5`,
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
      <Link className="nav-link p-0" to={`../admin/edit/${sel.metricName}`}>
        <small>Edit</small>
      </Link>
    </li>
  </ul>
);

export const PlotSubMenu = memo(_PlotSubMenu);
