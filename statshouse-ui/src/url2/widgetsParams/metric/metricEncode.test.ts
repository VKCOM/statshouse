// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import '@/testMock/matchMedia.mock';
import { PlotParams } from '../../queryParams';
import { getNewMetric } from './getNewMetric';
import { METRIC_TYPE, METRIC_VALUE_BACKEND_VERSION, PLOT_TYPE, QUERY_WHAT } from '../../../api/enum';
import { metricEncode } from './metricEncode';
import { promQLMetric } from '../../constants';

describe('@/urlStore widgetsParams/metric/metricEncode.ts', () => {
  test('@/urlEncodePlot', () => {
    const dParam: PlotParams = {
      ...getNewMetric(),
      id: '0',
    };
    const dParam2: PlotParams = {
      ...dParam,
      metricName: 'm1',
      customName: 'cn',
      customDescription: 'cd',
      promQL: 'promql',
      metricUnit: METRIC_TYPE.microsecond,
      what: [QUERY_WHAT.count, QUERY_WHAT.maxCountHost],
      customAgg: 5,
      groupBy: ['2', '3'],
      filterIn: { '0': ['val'] },
      filterNotIn: { '1': ['noval'] },
      numSeries: 10,
      backendVersion: METRIC_VALUE_BACKEND_VERSION.v1,
      yLock: {
        min: -100,
        max: 100,
      },
      maxHost: true,
      type: PLOT_TYPE.Event,
      events: ['0', '1'],
      eventsBy: ['0', '2'],
      eventsHide: ['0', '2'],
      totalLine: true,
      filledGraph: false,
      timeShifts: [200, 300],
    };
    expect(metricEncode(dParam)).toEqual([['s', '']]);
    expect(metricEncode(dParam, dParam)).toEqual([]);
    expect(metricEncode(dParam2, dParam)).toEqual([
      ['s', 'm1'],
      ['q', 'promql'],
      ['cn', 'cn'],
      ['cd', 'cd'],
      ['mt', 'mcs'],
      ['qw', 'count'],
      ['qw', 'max_count_host'],
      ['g', '5'],
      ['qb', '2'],
      ['qb', '3'],
      ['qf', '0-val'],
      ['qf', '1~noval'],
      ['n', '10'],
      ['v', '1'],
      ['yl', '-100'],
      ['yh', '100'],
      ['mh', '1'],
      ['qt', '1'],
      ['qe', '0'],
      ['qe', '1'],
      ['eb', '0'],
      ['eb', '2'],
      ['eh', '0'],
      ['eh', '2'],
      ['vtl', '1'],
      ['vfg', '0'],
      ['lts', '200'],
      ['lts', '300'],
    ]);
    expect(
      metricEncode(
        {
          ...dParam2,
          backendVersion: METRIC_VALUE_BACKEND_VERSION.v2,
          maxHost: false,
          totalLine: false,
          filledGraph: true,
        },
        dParam2
      )
    ).toEqual([
      ['v', '2'],
      ['mh', '0'],
      ['vtl', '0'],
      ['vfg', '1'],
    ]);
    expect(
      metricEncode(
        {
          ...dParam,
          promQL: 'promql',
        },
        dParam
      )
    ).toEqual([['q', 'promql']]);
    expect(
      metricEncode(
        {
          ...dParam,
          metricName: promQLMetric,
          promQL: '',
        },
        { ...dParam, promQL: 'promql' }
      )
    ).toEqual([['q', '']]);
    expect(
      metricEncode(
        {
          ...dParam,
          promQL: '',
        },
        { ...dParam, promQL: 'promql' }
      )
    ).toEqual([['q', '']]);
    expect(
      metricEncode({
        ...dParam,
        metricName: promQLMetric,
      })
    ).toEqual([['q', '']]);
  });
});
