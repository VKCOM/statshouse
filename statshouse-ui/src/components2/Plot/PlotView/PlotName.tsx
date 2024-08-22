// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo } from 'react';
import cn from 'classnames';
import { PlotKey } from 'url2';
import { useStatsHouseShallow } from 'store2';
import { getMetricName, getMetricWhat } from 'store2/helpers';

export type PlotNameProps = {
  plotKey: PlotKey;
  className?: string;
};
export function _PlotName({ plotKey, className }: PlotNameProps) {
  const { customName, metricName, what } = useStatsHouseShallow(({ params: { plots }, plotsData }) => {
    const plot = plots[plotKey];
    const plotData = plotsData[plotKey];
    return {
      customName: plot?.customName,
      metricName: getMetricName(plot, plotData),
      what: getMetricWhat(plot, plotData),
    };
  });

  if (customName) {
    return <span className={cn(className, 'text-body text-truncate')}>{customName}</span>;
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

export const PlotName = memo(_PlotName);
