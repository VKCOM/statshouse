// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback } from 'react';
import { useStatsHouseShallow } from 'store2';
import { getNewMetric, type PlotKey } from 'url2';

export type PlotControlNumSeriesProps = {
  plotKey: PlotKey;
};

const defaultNumSeries = getNewMetric().numSeries;

export function _PlotControlNumSeries({ plotKey }: PlotControlNumSeriesProps) {
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
  return (
    <select className="form-select" value={numSeries} onChange={onChange}>
      <option value="1">Top 1</option>
      <option value="2">Top 2</option>
      <option value="3">Top 3</option>
      <option value="4">Top 4</option>
      <option value="5">Top 5</option>
      <option value="10">Top 10</option>
      <option value="20">Top 20</option>
      <option value="30">Top 30</option>
      <option value="40">Top 40</option>
      <option value="50">Top 50</option>
      <option value="100">Top 100</option>
      <option value="0">All</option>
      <option value="-1">Bottom 1</option>
      <option value="-2">Bottom 2</option>
      <option value="-3">Bottom 3</option>
      <option value="-4">Bottom 4</option>
      <option value="-5">Bottom 5</option>
      <option value="-10">Bottom 10</option>
      <option value="-20">Bottom 20</option>
      <option value="-30">Bottom 30</option>
      <option value="-40">Bottom 40</option>
      <option value="-50">Bottom 50</option>
      <option value="-100">Bottom 100</option>
    </select>
  );
}

export const PlotControlNumSeries = memo(_PlotControlNumSeries);
