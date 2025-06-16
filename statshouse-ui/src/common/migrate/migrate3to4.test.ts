// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import '@/testMock/matchMedia.mock';
import { migrate3to4 } from './migrate3to4';
import { produce } from 'immer';
import { getDefaultParams, getNewGroup, getNewMetric, getNewMetricLayout, getNewVariable, QueryParams } from '@/url2';
import { promQLMetric } from '@/view/promQLMetric';

const paramsV3: QueryParams = {
  ...getDefaultParams(),
  timeRange: { absolute: true, from: 0, now: 1577826000, to: 1577826000, urlTo: 1577826000 },
  plots: {
    '0': { ...getNewMetric(), metricName: 'metric0', id: '0' },
    '1': { ...getNewMetric(), metricName: 'metric1', id: '1' },
    '2': { ...getNewMetric(), metricName: 'metric2', id: '2' },
    '3': {
      ...getNewMetric(),
      metricName: promQLMetric,
      promQL: 'promQL3',
      id: '3',
    },
  },
  orderPlot: ['3', '2', '1', '0'],
  groups: {
    '0': { ...getNewGroup(), id: '0', name: 'group0', count: 2, size: '3' },
    '1': { ...getNewGroup(), id: '1', name: 'group1', count: 0, size: '2' },
    '2': { ...getNewGroup(), id: '2', name: 'group2', count: 2, size: '2' },
    '3': { ...getNewGroup(), id: '3', name: 'group3', count: 0, size: '2' },
  },
  orderGroup: ['3', '2', '1', '0'],
  variables: {
    '0': { ...getNewVariable(), id: '0', name: 'var0' },
    '1': { ...getNewVariable(), id: '1', name: 'var1' },
    '2': { ...getNewVariable(), id: '2', name: 'var2' },
    '3': { ...getNewVariable(), id: '3', name: 'var3' },
  },
  orderVariables: ['3', '2', '1', '0'],
};

const paramsV4: QueryParams = {
  ...getDefaultParams(),
  timeRange: { absolute: true, from: 0, now: 1577826000, to: 1577826000, urlTo: 1577826000 },
  plots: {
    '0': {
      ...getNewMetric(),
      metricName: 'metric0',
      group: '0',
      layout: getNewMetricLayout(undefined, '3', { x: 8, y: 0 }),
      id: '0',
    },
    '1': {
      ...getNewMetric(),
      metricName: 'metric1',
      group: '0',
      layout: getNewMetricLayout(undefined, '3', { x: 0, y: 0 }),
      id: '1',
    },
    '2': {
      ...getNewMetric(),
      metricName: 'metric2',
      group: '2',
      layout: getNewMetricLayout(undefined, '2', { x: 12, y: 0 }),
      id: '2',
    },
    '3': {
      ...getNewMetric(),
      metricName: promQLMetric,
      promQL: 'promQL3',
      group: '2',
      layout: getNewMetricLayout(undefined, '2', { x: 0, y: 0 }),
      id: '3',
    },
  },
  orderPlot: [],
  groups: {
    '0': { ...getNewGroup(), id: '0', name: 'group0', count: 2, size: '3' },
    '1': { ...getNewGroup(), id: '1', name: 'group1', count: 0, size: '2' },
    '2': { ...getNewGroup(), id: '2', name: 'group2', count: 2, size: '2' },
    '3': { ...getNewGroup(), id: '3', name: 'group3', count: 0, size: '2' },
  },
  orderGroup: ['3', '2', '1', '0'],
  variables: {
    '0': { ...getNewVariable(), id: '0', name: 'var0' },
    '1': { ...getNewVariable(), id: '1', name: 'var1' },
    '2': { ...getNewVariable(), id: '2', name: 'var2' },
    '3': { ...getNewVariable(), id: '3', name: 'var3' },
  },
  orderVariables: ['3', '2', '1', '0'],
  version: '4',
};

describe('migrate3to4', () => {
  it('v3 input', () => {
    const paramsRes = produce(paramsV3, migrate3to4());
    expect(paramsRes).toEqual(paramsV4);
  });
});
