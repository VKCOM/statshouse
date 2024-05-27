import React from 'react';
import { type PlotKey, type PlotParams, promQLMetric, type QueryParams, type VariableItem } from 'store2';
import { PlotControlFilter } from './PlotControlFilter';
import { PlotControlPromQL } from './PlotControlPromQL';
import { type MetricMetaValue } from '../../../api/metric';
import { type TagKey } from '../../../api/enum';

export type PlotControlProps = {
  className?: string;
  plot?: PlotParams;
  setPlot?: (plot: PlotParams) => void;
  setParams?: (params: QueryParams) => void;
  params?: QueryParams;
  meta?: MetricMetaValue;
  metaLoading?: boolean;
  setUpdatedTag?: (plotKey: PlotKey, tagKey: TagKey | undefined, toggle: boolean) => void;
  tagsList?: Partial<Record<TagKey, VariableItem>>;
};
export function PlotControl(props: PlotControlProps) {
  if (props.plot?.promQL || props.plot?.metricName === promQLMetric) {
    return <PlotControlPromQL {...props}></PlotControlPromQL>;
  }
  return <PlotControlFilter {...props}></PlotControlFilter>;
}
