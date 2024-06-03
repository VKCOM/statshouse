import React from 'react';
import { type PlotParams, promQLMetric, QueryParams } from 'store2';
import { PlotControlFilter } from './PlotControlFilter';
import { PlotControlPromQL } from './PlotControlPromQL';
import { type MetricMetaValue } from '../../../api/metric';

export type PlotControlProps = {
  className?: string;
  plot?: PlotParams;
  setPlot?: (plot: PlotParams) => void;
  setParams?: (params: QueryParams) => void;
  params?: QueryParams;
  meta?: MetricMetaValue;
  metaLoading?: boolean;
};
export function PlotControl(props: PlotControlProps) {
  if (props.plot?.promQL || props.plot?.metricName === promQLMetric) {
    return <PlotControlPromQL {...props}></PlotControlPromQL>;
  }
  return <PlotControlFilter {...props}></PlotControlFilter>;
}
