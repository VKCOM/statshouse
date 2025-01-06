// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback, useMemo } from 'react';
import { useStatsHouseShallow } from '@/store2';
import { getNewMetric, type PlotKey } from '@/url2';
import { isMetricNumSeries, METRIC_NUM_SERIES, METRIC_NUM_SERIES_DESCRIPTION } from '@/api/enum';
import cn from 'classnames';

export type PlotControlNumSeriesProps = {
  plotKey: PlotKey;
};

const numSeriesList = Object.values(METRIC_NUM_SERIES).map((value) => ({
  value,
  description: METRIC_NUM_SERIES_DESCRIPTION[value],
}));

const defaultNumSeries = getNewMetric().numSeries;

export const PlotControlNumSeries = memo(function PlotControlNumSeries({ plotKey }: PlotControlNumSeriesProps) {
  const { numSeries, setPlot } = useStatsHouseShallow(({ params: { plots }, setPlot }) => ({
    numSeries: plots[plotKey]?.numSeries ?? defaultNumSeries,
    setPlot,
  }));
  const onChange = useCallback(
    (e: React.ChangeEvent<HTMLSelectElement>) => {
      const num = parseInt(e.target.value);
      setPlot(plotKey, (s) => {
        s.numSeries = num;
      });
    },
    [plotKey, setPlot]
  );
  const otherNum = useMemo(() => !isMetricNumSeries(numSeries), [numSeries]);

  return (
    <select className={cn('form-select', otherNum && 'border-danger')} value={numSeries} onChange={onChange}>
      {otherNum && (
        <option value={numSeries} disabled>
          Other
        </option>
      )}
      {numSeriesList.map(({ value, description }) => (
        <option key={value} value={value}>
          {description}
        </option>
      ))}
    </select>
  );
});
