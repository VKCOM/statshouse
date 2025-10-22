// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { type PlotParams, promQLMetric } from '@/url2';
import type { PlotData } from '../plotDataStore';
import { MetricMeta } from '../metricsMetaStore';
import { whatToWhatDesc } from '@/view/whatToWhatDesc';
import { QueryWhat } from '@/api/enum';

export function getMetricName(plot?: PlotParams, plotDataMetricName?: Pick<PlotData, 'metricName'>['metricName']) {
  if (!plot) {
    return '';
  }
  return (plot.metricName !== promQLMetric ? plot.metricName : plotDataMetricName) || `plot#${plot.id}`;
}

export function getMetricWhats(plot?: PlotParams, plotDataWhats?: Pick<PlotData, 'whats'>['whats']): QueryWhat[] {
  if (!plot) {
    return [];
  }
  return (plot.metricName === promQLMetric ? plotDataWhats : plot.what) || [];
}

export function getMetricWhat(plot?: PlotParams, plotDataWhats?: Pick<PlotData, 'whats'>['whats']) {
  return getMetricWhats(plot, plotDataWhats)
    .map((qw) => whatToWhatDesc(qw))
    .join(', ');
}

export function getMetricFullName(plot?: PlotParams, plotData?: Pick<PlotData, 'metricName' | 'whats'>) {
  if (!plot) {
    return '';
  }
  if (plot.customName) {
    return plot.customName;
  }
  const metricName = getMetricName(plot, plotData?.metricName);
  const metricWhat = getMetricWhat(plot, plotData?.whats);
  return metricName ? `${metricName}${metricWhat ? ': ' + metricWhat : ''}` : '';
}

export function getMetricMeta(metricMeta: Partial<Record<string, MetricMeta>>, plot?: PlotParams, plotData?: PlotData) {
  if (!plot) {
    return;
  }
  return metricMeta[(plot.metricName !== promQLMetric ? plot.metricName : plotData?.metricName) || ''];
}
