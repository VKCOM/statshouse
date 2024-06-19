import React from 'react';
import { PlotControlFilter } from './PlotControlFilter';
import { PlotControlPromQL } from './PlotControlPromQL';
import { PlotParams } from 'url2';
import { isPromQL } from 'store2/helpers';

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
