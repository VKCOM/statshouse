// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import { METRIC_TYPE, MetricType } from 'api/enum';
import { formatByMetricType } from 'common/formatByMetricType';
import { useMemo } from 'react';

export type PlotValueUnitProps = {
  unit: MetricType;
  value?: number | null;
};

export function PlotValueUnit({ unit, value }: PlotValueUnitProps) {
  const format = useMemo(() => formatByMetricType(unit), [unit]);
  if (unit === METRIC_TYPE.none && value != null) {
    return <span className="small text-secondary">{value}</span>;
  }
  return value != null ? (
    <span className="small text-secondary">
      {unit} {format(value)}&nbsp;({value})
    </span>
  ) : null;
}
