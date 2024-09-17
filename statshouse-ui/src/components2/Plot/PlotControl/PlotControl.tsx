// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import { PlotControlFilter } from './PlotControlFilter';
import { PlotControlPromQL } from './PlotControlPromQL';
import { PlotKey } from 'url2';
import { isPromQL } from 'store2/helpers';
import { useStatsHouse } from 'store2';

export type PlotControlProps = {
  className?: string;
  plotKey: PlotKey;
};
export function PlotControl(props: PlotControlProps) {
  const isProm = useStatsHouse(({ params: { tabNum, plots } }) => isPromQL(plots[tabNum]));
  const isPlot = useStatsHouse(({ params: { tabNum, plots } }) => !!plots[tabNum]);
  if (!isPlot) {
    return null;
  }
  if (isProm) {
    return <PlotControlPromQL {...props}></PlotControlPromQL>;
  }
  return <PlotControlFilter {...props}></PlotControlFilter>;
}
