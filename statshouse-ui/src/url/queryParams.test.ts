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
    expect(p.toString()).toEqual('');
    const s = decodeParams(p);
    expect(s).toEqual(testParam);
  });

  test('encodeParams -> decodeParams default', () => {
    const defaultParam: QueryParams = {
      live: false,
      theme: undefined,
      dashboard: {
        dashboard_id: undefined,
        groupInfo: [{ name: 'qwe', count: 2, show: true, size: 3 }],
        description: '',
        name: '',
        version: undefined,
      },
      tabNum: -1,
      plots: [
        {
          metricName: 'test',
          customName: 'test name',
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
        },
        {
          metricName: 'test',
          customName: 'test name 2',
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
            [0, 2],
            [1, 2],
          ],
          description: 'variable',
          args: { groupBy: true, negative: true },
        },
      ],
    };
    const testParam: QueryParams = {
      live: false,
      dashboard: {
        dashboard_id: undefined,
        groupInfo: [{ name: 'qwe', count: 2, show: true, size: 2 }],
        description: '',
        name: '',
        version: undefined,
      },
      tabNum: -1,
      plots: [
        {
          metricName: 'test',
          customName: 'test name',
          what: ['count_norm', 'avg'],
          type: 0,
          numSeries: 5,
          promQL: '',
          maxHost: false,
          customAgg: 1,
          groupBy: ['_s', '1'],
          filterIn: {
            1: ['p', 'a', 's'],
          },
          filterNotIn: {
            2: ['s', 'r'],
          },
          useV2: true,
          yLock: { min: 10, max: 1000 },
          events: [1],
          eventsBy: ['0', '1'],
          eventsHide: ['1'],
        },
        {
          metricName: 'test',
          customName: 'test name 2',
          what: ['count_norm', 'avg'],
          type: 0,
          numSeries: 5,
          promQL: '',
          maxHost: true,
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
            [0, 3],
            [1, 2],
            [2, -1],
          ],
          description: 'variable',
          args: { groupBy: true, negative: true },
        },
        {
          name: 'var2',
          values: ['3', '4'],
          link: [
            [3, 3],
            [3, 2],
            [3, -1],
          ],
          description: 'variable2',
          args: { groupBy: false, negative: false },
        },
      ],
    };
    const p = encodeParams(testParam, defaultParam);
    expect([...p.entries()]).toEqual([
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
      ['qf', '1-p'],
      ['qf', '1-a'],
      ['qf', '1-s'],
      ['qf', '2~s'],
      ['qf', '2~r'],
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
      ['t1.qf', '1-p'],
      ['t1.qf', '1-a'],
      ['t1.qf', '2~s'],
      ['t1.qf', '2~r'],
      ['t1.yl', '10'],
      ['t1.yh', '1000'],
      ['t1.mh', '1'],
      ['t1.qe', '1'],
      ['t1.eb', '0'],
      ['t1.eb', '1'],
      ['t1.eh', '1'],
      ['ts', '3600'],
      ['ts', '1800'],
      ['fs', '0.1-1.2-2.3'],
      ['fs', '0.2-2.4'],
      ['v0.n', 'var1'],
      ['v0.d', 'variable'],
      ['v0.l', '0.3-1.2-2._s'],
      ['v.var1', '1'],
      ['v.var1', '2'],
      ['v.var1.g', '1'],
      ['v.var1.nv', '1'],
      ['v1.n', 'var2'],
      ['v1.d', 'variable2'],
      ['v1.l', '3.3-3.2-3._s'],
      ['v.var2', '3'],
      ['v.var2', '4'],
    ]);
    expect(p.toString()).toEqual(
      'g0.t=qwe&g0.n=2&t=ed&f=1800&s=test&cn=test+name&qw=count_norm&qw=avg&g=1&qb=_s&qb=1&qf=1-p&qf=1-a&qf=1-s&qf=2%7Es&qf=2%7Er&yl=10&yh=1000&qe=1&eb=0&eb=1&eh=1&t1.s=test&t1.cn=test+name+2&t1.qw=count_norm&t1.qw=avg&t1.g=1&t1.qb=_s&t1.qb=1&t1.qf=1-p&t1.qf=1-a&t1.qf=2%7Es&t1.qf=2%7Er&t1.yl=10&t1.yh=1000&t1.mh=1&t1.qe=1&t1.eb=0&t1.eb=1&t1.eh=1&ts=3600&ts=1800&fs=0.1-1.2-2.3&fs=0.2-2.4&v0.n=var1&v0.d=variable&v0.l=0.3-1.2-2._s&v.var1=1&v.var1=2&v.var1.g=1&v.var1.nv=1&v1.n=var2&v1.d=variable2&v1.l=3.3-3.2-3._s&v.var2=3&v.var2=4'
    );
    const s = decodeParams(p, defaultParam);
    expect(s).toEqual(testParam);
  });
});
