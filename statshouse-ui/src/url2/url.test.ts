// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import '@/testMock/matchMedia.mock';
import type { QueryParams } from './queryParams';
import { urlEncode } from './urlEncode';
import { urlDecode } from './urlDecode';
import { getDefaultParams, getNewGroup, getNewVariable } from './getDefault';
import { arrToObj, getNewMetricLayout, toTreeObj } from './index';
import { getNewMetric } from './widgetsParams';
import { promQLMetric } from '@/view/promQLMetric';

jest.useFakeTimers().setSystemTime(new Date('2020-01-01 00:00:00'));

const params: QueryParams = {
  ...getDefaultParams(),
  timeRange: { absolute: true, from: 0, now: 1577826000, to: 1577826000, urlTo: 1577826000 },
  plots: {
    '0': { ...getNewMetric(), metricName: 'metric0', group: '0', layout: getNewMetricLayout(), id: '0' },
    '1': { ...getNewMetric(), metricName: 'metric1', group: '1', layout: getNewMetricLayout(), id: '1' },
    '2': { ...getNewMetric(), metricName: 'metric2', group: '2', layout: getNewMetricLayout(), id: '2' },
    '3': {
      ...getNewMetric(),
      metricName: promQLMetric,
      promQL: 'promQL3',
      group: '3',
      layout: getNewMetricLayout(),
      id: '3',
    },
  },
  orderPlot: ['3', '2', '1', '0'],
  groups: {
    '0': { ...getNewGroup(), id: '0', name: 'group0', count: 0 },
    '1': { ...getNewGroup(), id: '1', name: 'group1', count: 0 },
    '2': { ...getNewGroup(), id: '2', name: 'group2', count: 0 },
    '3': { ...getNewGroup(), id: '3', name: 'group3', count: 0 },
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
const params2: QueryParams = {
  ...getDefaultParams(),
  timeRange: { absolute: true, from: 0, now: 1577826000, to: 1577826000, urlTo: 1577826000 },
  plots: {
    '1': { ...getNewMetric(), metricName: 'metric1', group: '0', layout: getNewMetricLayout(), id: '1' },
    '2': { ...getNewMetric(), metricName: 'metric2', group: '2', layout: getNewMetricLayout(), id: '2' },
    '3': {
      ...getNewMetric(),
      metricName: promQLMetric,
      promQL: 'promQL3',
      group: '3',
      layout: getNewMetricLayout(),
      id: '3',
    },
  },
  orderPlot: ['3', '2', '1'],
  groups: {
    '0': { ...getNewGroup(), id: '0', name: 'group0', count: 0 },
    '2': { ...getNewGroup(), id: '2', name: 'group2', count: 0 },
    '3': { ...getNewGroup(), id: '3', name: 'group3', count: 0 },
  },
  orderGroup: ['3', '2', '0'],
  variables: {
    '0': { ...getNewVariable(), id: '0', name: 'var0' },
    '1': { ...getNewVariable(), id: '1', name: 'var1' },
    '3': { ...getNewVariable(), id: '3', name: 'var3' },
  },
  orderVariables: ['3', '1', '0'],
  version: '4',
};

describe('@/urlStore', () => {
  test('@/urlEncode => urlDecode', () => {
    // console.log(urlEncode(params));
    expect(urlDecode(toTreeObj(arrToObj(urlEncode(params))), params)).toEqual(params);
  });
  test('@/urlEncode => urlDecode save', () => {
    expect(urlDecode(toTreeObj(arrToObj(urlEncode(params2, params))), params)).toEqual(params2);
    expect(urlDecode(toTreeObj(arrToObj(urlEncode(params, params2))), params2)).toEqual(params);
  });
});
