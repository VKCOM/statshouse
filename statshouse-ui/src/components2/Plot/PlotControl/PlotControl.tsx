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
  if (isProm) {
    return <PlotControlPromQL {...props}></PlotControlPromQL>;
  }
  return <PlotControlFilter {...props}></PlotControlFilter>;
}
