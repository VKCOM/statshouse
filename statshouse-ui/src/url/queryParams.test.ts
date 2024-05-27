// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { decodeParams, encodeParams, QueryParams } from './queryParams';
import { TIME_RANGE_KEYS_TO } from '../common/TimeRange';

let locationSearch = '';
// window.location search mock
Object.defineProperty(window, 'location', {
  configurable: true,
  get() {
    return { search: locationSearch };
  },
});

describe('queryParams.ts', () => {
  beforeEach(() => {
    locationSearch = '';
  });

  test('encodeParams -> decodeParams base', () => {
    const testParam: QueryParams = {
      live: false,
      theme: undefined,
      dashboard: {
        dashboard_id: undefined,
        groupInfo: [],
        description: '',
        name: '',
        version: undefined,
      },
      tabNum: 0,
      plots: [],
      timeRange: { to: TIME_RANGE_KEYS_TO.default, from: 0 },
      tagSync: [],
      timeShifts: [],
      eventFrom: 0,
      variables: [],
    };
    const p = encodeParams(testParam);
    expect(new URLSearchParams(p).toString()).toEqual('');
    const s = decodeParams(p);
    expect(s).toEqual(testParam);
  });

  test('encodeParams -> decodeParams default', () => {
    const defaultParam: QueryParams = {
      live: false,
      theme: undefined,
      dashboard: {
        dashboard_id: undefined,
        groupInfo: [{ name: 'qwe', count: 2, show: true, size: '3', description: '' }],
        description: '',
        name: '',
        version: undefined,
      },
      tabNum: -1,
      plots: [
        {
          id: '0',
          metricName: 'test',
          customName: 'test name',
          customDescription: '',
          what: ['count_norm', 'avg'],
          type: 0,
          numSeries: 5,
          promQL: '',
          maxHost: false,
          customAgg: 1,
          groupBy: ['_s', '1'],
          filterIn: {
            1: ['p', 'a'],
          },
          filterNotIn: {
            2: ['s', 'r'],
          },
          useV2: true,
          yLock: { min: 10, max: 1000 },
          events: [1],
          eventsBy: ['0', '1'],
          eventsHide: ['1'],
          totalLine: false,
          filledGraph: true,
        },
        {
          id: '1',
          metricName: 'test',
          customName: 'test name 2',
          customDescription: '',
          what: ['count_norm', 'avg'],
          type: 0,
          numSeries: 5,
          promQL: '',
          maxHost: false,
          customAgg: 1,
          groupBy: ['_s', '1'],
          filterIn: {
            1: ['p', 'a'],
          },
          filterNotIn: {
            2: ['s', 'r'],
          },
          useV2: true,
          yLock: { min: 10, max: 1000 },
          events: [1],
          eventsBy: ['0', '1'],
          eventsHide: ['1'],
          totalLine: true,
          filledGraph: true,
        },
      ],
      timeRange: { to: 3333, from: 3600 },
      tagSync: [],
      timeShifts: [3600],
      eventFrom: 0,
      variables: [
        {
          name: 'var1',
          values: ['1', '2'],
          link: [
            ['0', '2'],
            ['1', '2'],
          ],
          description: 'variable',
          args: { groupBy: true, negative: true },
          source: [],
        },
      ],
    };
    const testParam: QueryParams = {
      live: false,
      theme: undefined,
      dashboard: {
        dashboard_id: undefined,
        groupInfo: [{ name: 'qwe', count: 2, show: true, size: '2', description: '' }],
        description: '',
        name: '',
        version: undefined,
      },
      tabNum: -1,
      plots: [
        {
          id: '0',
          metricName: 'test',
          customName: 'test name',
          customDescription: '',
          what: ['count_norm', 'avg'],
          type: 0,
          numSeries: 5,
          promQL: '',
          maxHost: false,
          customAgg: 1,
          groupBy: ['_s', '1'],
          filterIn: {
            1: ['a', 'p', 's'],
          },
          filterNotIn: {
            2: ['r', 's'],
          },
          useV2: true,
          yLock: { min: 10, max: 1000 },
          events: [1],
          eventsBy: ['0', '1'],
          eventsHide: ['1'],
          totalLine: false,
          filledGraph: true,
        },
        {
          id: '1',
          metricName: 'test',
          customName: 'test name 2',
          customDescription: '',
          what: ['count_norm', 'avg'],
          type: 0,
          numSeries: 5,
          promQL: '',
          maxHost: true,
          customAgg: 1,
          groupBy: ['_s', '1'],
          filterIn: {
            1: ['a', 'p'],
          },
          filterNotIn: {
            2: ['r', 's'],
          },
          useV2: true,
          yLock: { min: 10, max: 1000 },
          events: [1],
          eventsBy: ['0', '1'],
          eventsHide: ['1'],
          totalLine: true,
          filledGraph: true,
        },
      ],
      timeRange: { to: 'ed', from: 1800 },
      tagSync: [
        [1, 2, 3],
        [2, null, 4],
      ],
      timeShifts: [3600, 1800],
      eventFrom: 0,
      variables: [
        {
          name: 'var1',
          values: ['1', '2'],
          link: [
            ['0', '3'],
            ['1', '2'],
            ['2', '_s'],
          ],
          description: 'variable1',
          args: { groupBy: true, negative: true },
          source: [],
        },
        {
          name: 'var2',
          values: ['3', '4'],
          link: [
            ['3', '3'],
            ['3', '2'],
            ['3', '_s'],
          ],
          description: 'variable2',
          args: { groupBy: false, negative: false },
          source: [],
        },
      ],
    };
    const p = encodeParams(testParam, defaultParam);
    expect(p).toEqual([
      ['g0.t', 'qwe'],
      ['g0.n', '2'],
      ['t', 'ed'],
      ['f', '1800'],
      ['s', 'test'],
      ['cn', 'test name'],
      ['qw', 'count_norm'],
      ['qw', 'avg'],
      ['g', '1'],
      ['qb', '_s'],
      ['qb', '1'],
      ['qf', '1-a'],
      ['qf', '1-p'],
      ['qf', '1-s'],
      ['qf', '2~r'],
      ['qf', '2~s'],
      ['yl', '10'],
      ['yh', '1000'],
      ['qe', '1'],
      ['eb', '0'],
      ['eb', '1'],
      ['eh', '1'],
      ['t1.s', 'test'],
      ['t1.cn', 'test name 2'],
      ['t1.qw', 'count_norm'],
      ['t1.qw', 'avg'],
      ['t1.g', '1'],
      ['t1.qb', '_s'],
      ['t1.qb', '1'],
      ['t1.qf', '1-a'],
      ['t1.qf', '1-p'],
      ['t1.qf', '2~r'],
      ['t1.qf', '2~s'],
      ['t1.yl', '10'],
      ['t1.yh', '1000'],
      ['t1.mh', '1'],
      ['t1.qe', '1'],
      ['t1.eb', '0'],
      ['t1.eb', '1'],
      ['t1.eh', '1'],
      ['t1.vtl', '1'],
      ['ts', '3600'],
      ['ts', '1800'],
      ['fs', '0.1-1.2-2.3'],
      ['fs', '0.2-2.4'],
      ['v0.n', 'var1'],
      ['v0.d', 'variable1'],
      ['v0.l', '0.3-1.2-2._s'],
      ['v1.n', 'var2'],
      ['v1.d', 'variable2'],
      ['v1.l', '3.3-3.2-3._s'],
      ['v.var2', '3'],
      ['v.var2', '4'],
    ]);
    expect(new URLSearchParams(p).toString()).toEqual(
      'g0.t=qwe&g0.n=2&t=ed&f=1800&s=test&cn=test+name&qw=count_norm&qw=avg&g=1&qb=_s&qb=1&qf=1-a&qf=1-p&qf=1-s&qf=2%7Er&qf=2%7Es&yl=10&yh=1000&qe=1&eb=0&eb=1&eh=1&t1.s=test&t1.cn=test+name+2&t1.qw=count_norm&t1.qw=avg&t1.g=1&t1.qb=_s&t1.qb=1&t1.qf=1-a&t1.qf=1-p&t1.qf=2%7Er&t1.qf=2%7Es&t1.yl=10&t1.yh=1000&t1.mh=1&t1.qe=1&t1.eb=0&t1.eb=1&t1.eh=1&t1.vtl=1&ts=3600&ts=1800&fs=0.1-1.2-2.3&fs=0.2-2.4&v0.n=var1&v0.d=variable1&v0.l=0.3-1.2-2._s&v1.n=var2&v1.d=variable2&v1.l=3.3-3.2-3._s&v.var2=3&v.var2=4'
    );
    const s = decodeParams(p, defaultParam);
    expect(s).toEqual(testParam);
  });

  test('encodeParams -> decodeParams base variable', () => {
    const defaultParam: QueryParams = {
      live: false,
      theme: undefined,
      dashboard: {
        dashboard_id: undefined,
        groupInfo: [],
        description: '',
        name: '',
        version: undefined,
      },
      timeRange: { to: TIME_RANGE_KEYS_TO.default, from: 0 },
      eventFrom: 0,
      tagSync: [],
      plots: [],
      timeShifts: [],
      tabNum: 0,
      variables: [],
    };
    const testParam: QueryParams = {
      live: false,
      theme: undefined,
      dashboard: {
        dashboard_id: undefined,
        groupInfo: [],
        description: '',
        name: '',
        version: undefined,
      },
      timeRange: { to: 120, from: 60 },
      eventFrom: 0,
      tagSync: [],
      plots: [],
      timeShifts: [],
      tabNum: 0,
      variables: [
        {
          name: 'var1',
          values: ['value1', 'value2'],
          link: [],
          args: {
            groupBy: false,
            negative: false,
          },
          description: 'variable one',
          source: [
            {
              metric: 'metric1',
              tag: '0',
              filterIn: { '2': ['filter_tag_2_1', 'filter_tag_2_2'], _s: ['filter_string'] },
              filterNotIn: { '3': ['filter_tag_3_1'] },
            },
          ],
        },
        {
          name: 'var2',
          values: ['value2_1', 'value2_2'],
          link: [],
          args: {
            groupBy: true,
            negative: true,
          },
          description: 'variable two',
          source: [
            {
              metric: 'metric2',
              tag: '0',
              filterIn: { '2': ['filter_tag_2_1', 'filter_tag_2_2'], _s: ['filter_string'] },
              filterNotIn: { '3': ['filter_tag_3_1'] },
            },
          ],
        },
      ],
    };

    const p = encodeParams(testParam, defaultParam);
    expect(p).toEqual([
      ['t', '120'],
      ['f', '60'],
      ['v0.n', 'var1'],
      ['v0.d', 'variable one'],
      ['v0.s0.s', 'metric1'],
      ['v0.s0.t', '0'],
      ['v0.s0.qf', '2-filter_tag_2_1'],
      ['v0.s0.qf', '2-filter_tag_2_2'],
      ['v0.s0.qf', '_s-filter_string'],
      ['v0.s0.qf', '3~filter_tag_3_1'],
      ['v1.n', 'var2'],
      ['v1.d', 'variable two'],
      ['v1.s0.s', 'metric2'],
      ['v1.s0.t', '0'],
      ['v1.s0.qf', '2-filter_tag_2_1'],
      ['v1.s0.qf', '2-filter_tag_2_2'],
      ['v1.s0.qf', '_s-filter_string'],
      ['v1.s0.qf', '3~filter_tag_3_1'],
      ['v.var1', 'value1'],
      ['v.var1', 'value2'],
      ['v.var2', 'value2_1'],
      ['v.var2', 'value2_2'],
      ['v.var2.g', '1'],
      ['v.var2.nv', '1'],
    ]);
    const s = decodeParams(p, defaultParam);
    expect(s).toEqual(testParam);
  });

  test('encodeParams -> decodeParams default variable', () => {
    const defaultParam: QueryParams = {
      live: false,
      theme: undefined,
      dashboard: {
        dashboard_id: undefined,
        groupInfo: [],
        description: '',
        name: '',
        version: undefined,
      },
      timeRange: { to: 120, from: 60 },
      eventFrom: 0,
      tagSync: [],
      plots: [],
      timeShifts: [],
      tabNum: 0,
      variables: [
        {
          name: 'var1',
          values: ['value1', 'value2'],
          link: [],
          args: {
            groupBy: false,
            negative: false,
          },
          description: 'variable one',
          source: [
            {
              metric: 'metric1',
              tag: '0',
              filterIn: { '2': ['filter_tag_2_1', 'filter_tag_2_2'], _s: ['filter_string'] },
              filterNotIn: { '3': ['filter_tag_3_1'] },
            },
          ],
        },
        {
          name: 'var2',
          values: ['value2_1', 'value2_2'],
          link: [],
          args: {
            groupBy: true,
            negative: true,
          },
          description: 'variable two',
          source: [
            {
              metric: 'metric2',
              tag: '0',
              filterIn: { '2': ['filter_tag_2_1', 'filter_tag_2_2'], _s: ['filter_string'] },
              filterNotIn: { '3': ['filter_tag_3_1'] },
            },
          ],
        },
      ],
    };
    const testParam: QueryParams = {
      live: false,
      theme: undefined,
      dashboard: {
        dashboard_id: undefined,
        groupInfo: [],
        description: '',
        name: '',
        version: undefined,
      },
      timeRange: { to: 120, from: 60 },
      eventFrom: 0,
      tagSync: [],
      plots: [],
      timeShifts: [],
      tabNum: 0,
      variables: [
        {
          name: 'var1',
          values: ['value1', 'value2'],
          link: [],
          args: {
            groupBy: false,
            negative: true,
          },
          description: 'variable one',
          source: [
            {
              metric: 'metric1',
              tag: '0',
              filterIn: { '2': ['filter_tag_2_1', 'filter_tag_2_2'], _s: ['filter_string'] },
              filterNotIn: { '3': ['filter_tag_3_1'] },
            },
          ],
        },
        {
          name: 'var2',
          values: ['value2_1', 'value2_2'],
          link: [],
          args: {
            groupBy: false,
            negative: true,
          },
          description: 'variable two',
          source: [
            {
              metric: 'metric2',
              tag: '0',
              filterIn: { _s: ['filter_string'] },
              filterNotIn: { '3': ['filter_tag_3_1', 'filter_tag_3_2'] },
            },
          ],
        },
      ],
    };

    const p = encodeParams(testParam, defaultParam);
    expect(p).toEqual([
      ['v0.n', 'var1'],
      ['v0.d', 'variable one'],
      ['v0.s0.s', 'metric1'],
      ['v0.s0.t', '0'],
      ['v0.s0.qf', '2-filter_tag_2_1'],
      ['v0.s0.qf', '2-filter_tag_2_2'],
      ['v0.s0.qf', '_s-filter_string'],
      ['v0.s0.qf', '3~filter_tag_3_1'],
      ['v1.n', 'var2'],
      ['v1.d', 'variable two'],
      ['v1.s0.s', 'metric2'],
      ['v1.s0.t', '0'],
      ['v1.s0.qf', '_s-filter_string'],
      ['v1.s0.qf', '3~filter_tag_3_1'],
      ['v1.s0.qf', '3~filter_tag_3_2'],
      ['v.var1.nv', '1'],
      ['v.var2.g', '0'],
    ]);
    const s = decodeParams(p, defaultParam);
    expect(s).toEqual(testParam);
  });
  test('encodeParams -> decodeParams default variable only value', () => {
    const defaultParam: QueryParams = {
      live: false,
      theme: undefined,
      dashboard: {
        dashboard_id: undefined,
        groupInfo: [],
        description: '',
        name: '',
        version: undefined,
      },
      timeRange: { to: 120, from: 60 },
      eventFrom: 0,
      tagSync: [],
      plots: [],
      timeShifts: [],
      tabNum: 0,
      variables: [
        {
          name: 'var1',
          values: ['value1', 'value2'],
          link: [],
          args: {
            groupBy: false,
            negative: false,
          },
          description: 'variable one',
          source: [
            {
              metric: 'metric1',
              tag: '0',
              filterIn: { '2': ['filter_tag_2_1', 'filter_tag_2_2'], _s: ['filter_string'] },
              filterNotIn: { '3': ['filter_tag_3_1'] },
            },
          ],
        },
        {
          name: 'var2',
          values: ['value2_1', 'value2_2'],
          link: [],
          args: {
            groupBy: true,
            negative: true,
          },
          description: 'variable two',
          source: [
            {
              metric: 'metric2',
              tag: '0',
              filterIn: { '2': ['filter_tag_2_1', 'filter_tag_2_2'], _s: ['filter_string'] },
              filterNotIn: { '3': ['filter_tag_3_1'] },
            },
          ],
        },
      ],
    };
    const testParam: QueryParams = {
      live: false,
      theme: undefined,
      dashboard: {
        dashboard_id: undefined,
        groupInfo: [],
        description: '',
        name: '',
        version: undefined,
      },
      timeRange: { to: 120, from: 60 },
      eventFrom: 0,
      tagSync: [],
      plots: [],
      timeShifts: [],
      tabNum: 0,
      variables: [
        {
          name: 'var1',
          values: ['value1'],
          link: [],
          args: {
            groupBy: true,
            negative: false,
          },
          description: 'variable one',
          source: [
            {
              metric: 'metric1',
              tag: '0',
              filterIn: { '2': ['filter_tag_2_1', 'filter_tag_2_2'], _s: ['filter_string'] },
              filterNotIn: { '3': ['filter_tag_3_1'] },
            },
          ],
        },
        {
          name: 'var2',
          values: [],
          link: [],
          args: {
            groupBy: true,
            negative: false,
          },
          description: 'variable two',
          source: [
            {
              metric: 'metric2',
              tag: '0',
              filterIn: { '2': ['filter_tag_2_1', 'filter_tag_2_2'], _s: ['filter_string'] },
              filterNotIn: { '3': ['filter_tag_3_1'] },
            },
          ],
        },
      ],
    };

    const p = encodeParams(testParam, defaultParam);
    expect(p).toEqual([
      ['v.var1', 'value1'],
      ['v.var1.g', '1'],
      ['v.var2', '\u0007'],
      ['v.var2.nv', '0'],
    ]);
    const s = decodeParams(p, defaultParam);
    expect(s).toEqual(testParam);
  });
});
