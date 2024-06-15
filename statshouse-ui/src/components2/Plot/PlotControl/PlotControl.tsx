import React from 'react';
import { isPromQL, type PlotParams } from 'store2';
import { PlotControlFilter } from './PlotControlFilter';
import { PlotControlPromQL } from './PlotControlPromQL';

export type PlotControlProps = {
  className?: string;
  plot?: PlotParams;
};
export function PlotControl(props: PlotControlProps) {
  if (isPromQL(props.plot)) {
    return <PlotControlPromQL {...props}></PlotControlPromQL>;
  }
  return <PlotControlFilter {...props}></PlotControlFilter>;
}
