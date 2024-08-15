// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useMemo } from 'react';
import cn from 'classnames';
import { type PlotParams, promQLMetric } from 'url2';
import { type PlotData } from 'store2/plotDataStore';
import { whatToWhatDesc } from 'view/api';

export type PlotNameProps = {
  plot?: PlotParams;
  plotData?: PlotData;
  className?: string;
};
export function PlotName({ plot, plotData, className }: PlotNameProps) {
  const metricName = useMemo(
    () => (plot?.metricName !== promQLMetric ? plot?.metricName : plotData?.metricName),
    [plot?.metricName, plotData?.metricName]
  );
  const what = useMemo(
    () =>
      plot?.metricName === promQLMetric
        ? plotData?.whats.map((qw) => whatToWhatDesc(qw)).join(', ')
        : plot?.what.map((qw) => whatToWhatDesc(qw)).join(', '),
    [plot?.metricName, plot?.what, plotData?.whats]
  );

  if (plot?.customName) {
    return <span className={cn(className, 'text-body text-truncate')}>{plot?.customName}</span>;
  }
  if (metricName) {
    return (
      <span className={cn(className)}>
        <span className="text-body text-truncate">{metricName}</span>
        {!!what && <span className="text-secondary text-truncate">:&nbsp;{what}</span>}
      </span>
    );
  }
  return <span className={cn(className)}>&nbsp;</span>;
}
