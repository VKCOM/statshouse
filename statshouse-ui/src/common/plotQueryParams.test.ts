// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import './../testMock/matchMedia.mock';
import { defaultParams, readParams, writeParams } from './plotQueryParams';

describe('plotQueryParams', () => {
  test('readParams old key format', () => {
    const params = new URLSearchParams(
      '?f=-172800&t=1660038496&t3.yh=5800&t2.yh=161&tn=-1&t2.yl=131&t1.yl=50&t1.yh=169&s=api_methods&qb=key1&qf=key1-messages&qf=key1-stats&qf=key2%7EgetById&qf=key2%7EgetLongPollServer&qf=key2%7EgetSuggestedContacts&yl=118794.27083333333&yh=140183.15972222222&t1.s=api_methods&t1.qw=min&t1.qb=key1&t1.qf=key0-production&t1.qf=key2-add&t1.qf=key2-getById&t2.s=api_methods&t2.qw=p90&t2.g=1&t2.qf=key0-production&t2.qf=key2-add&t2.qf=key2-trackEvents&t2.n=10&t2.ts=-86400'
    );
    expect(
      readParams({ timeRange: { to: -1, from: 0 }, tagSync: [], plots: [], tabNum: 0 }, params, defaultParams)
    ).toEqual({
      timeRange: { to: 1660038496, from: -172800 },
      tagSync: [],
      plots: [
        {
          customAgg: 0,
          filterIn: {
            key1: ['messages', 'stats'],
          },
          filterNotIn: {
            key2: ['getById', 'getLongPollServer', 'getSuggestedContacts'],
          },
          groupBy: ['key1'],
          metricName: 'api_methods',
          numSeries: 5,
          timeShifts: [],
          useV2: true,
          what: ['count_norm'],
          yLock: {
            max: 140183,
            min: 118794,
          },
        },
        {
          customAgg: 0,
          filterIn: {
            key0: ['production'],
            key2: ['add', 'getById'],
          },
          filterNotIn: {},
          groupBy: ['key1'],
          metricName: 'api_methods',
          numSeries: 5,
          timeShifts: [],
          useV2: true,
          what: ['min'],
          yLock: {
            max: 169,
            min: 50,
          },
        },
        {
          customAgg: 1,
          filterIn: {
            key0: ['production'],
            key2: ['add', 'trackEvents'],
          },
          filterNotIn: {},
          groupBy: [],
          metricName: 'api_methods',
          numSeries: 10,
          timeShifts: [-86400],
          useV2: true,
          what: ['p90'],
          yLock: {
            max: 161,
            min: 131,
          },
        },
      ],
      tabNum: -1,
    });
  });
  test('readParams', () => {
    const params = new URLSearchParams(
      '?f=-172800&t=1660038496&t3.yh=5800&t2.yh=161&tn=-1&t2.yl=131&t1.yl=50&t1.yh=169&s=api_methods&qb=key1&qf=1-messages&qf=1-stats&qf=key2%7EgetById&qf=2%7EgetLongPollServer&qf=2%7EgetSuggestedContacts&yl=118794.27083333333&yh=140183.15972222222&t1.s=api_methods&t1.qw=min&t1.qb=key1&t1.qf=0-production&t1.qf=2-add&t1.qf=2-getById&t2.s=api_methods&t2.qw=p90&t2.g=1&t2.qf=0-production&t2.qf=2-add&t2.qf=2-trackEvents&t2.n=10&t2.ts=-86400'
    );
    expect(
      readParams({ timeRange: { to: -1, from: 0 }, tagSync: [], plots: [], tabNum: 0 }, params, defaultParams)
    ).toEqual({
      timeRange: { to: 1660038496, from: -172800 },
      tagSync: [],
      plots: [
        {
          customAgg: 0,
          filterIn: {
            key1: ['messages', 'stats'],
          },
          filterNotIn: {
            key2: ['getById', 'getLongPollServer', 'getSuggestedContacts'],
          },
          groupBy: ['key1'],
          metricName: 'api_methods',
          numSeries: 5,
          timeShifts: [],
          useV2: true,
          what: ['count_norm'],
          yLock: {
            max: 140183,
            min: 118794,
          },
        },
        {
          customAgg: 0,
          filterIn: {
            key0: ['production'],
            key2: ['add', 'getById'],
          },
          filterNotIn: {},
          groupBy: ['key1'],
          metricName: 'api_methods',
          numSeries: 5,
          timeShifts: [],
          useV2: true,
          what: ['min'],
          yLock: {
            max: 169,
            min: 50,
          },
        },
        {
          customAgg: 1,
          filterIn: {
            key0: ['production'],
            key2: ['add', 'trackEvents'],
          },
          filterNotIn: {},
          groupBy: [],
          metricName: 'api_methods',
          numSeries: 10,
          timeShifts: [-86400],
          useV2: true,
          what: ['p90'],
          yLock: {
            max: 161,
            min: 131,
          },
        },
      ],
      tabNum: -1,
    });
  });
  test('writeParams', () => {
    const params = writeParams(
      {
        timeRange: { to: 1660038496, from: -172800 },
        tagSync: [],
        plots: [
          {
            customAgg: 0,
            filterIn: {
              key1: ['messages', 'stats'],
            },
            filterNotIn: {
              key2: ['getById', 'getLongPollServer', 'getSuggestedContacts'],
            },
            groupBy: ['key1'],
            metricName: 'api_methods',
            numSeries: 5,
            timeShifts: [],
            useV2: true,
            what: ['count_norm'],
            yLock: {
              max: 140183,
              min: 118794,
            },
          },
          {
            customAgg: 0,
            filterIn: {
              key0: ['production'],
              key2: ['add', 'getById'],
            },
            filterNotIn: {},
            groupBy: ['key1'],
            metricName: 'api_methods',
            numSeries: 5,
            timeShifts: [],
            useV2: true,
            what: ['min'],
            yLock: {
              max: 169,
              min: 50,
            },
          },
          {
            customAgg: 1,
            filterIn: {
              key0: ['production'],
              key2: ['add', 'trackEvents'],
            },
            filterNotIn: {},
            groupBy: [],
            metricName: 'api_methods',
            numSeries: 10,
            timeShifts: [-86400],
            useV2: true,
            what: ['p90'],
            yLock: {
              max: 161,
              min: 131,
            },
          },
        ],
        tabNum: -1,
      },
      new URLSearchParams('?other=param'),
      defaultParams
    );
    expect(params.toString()).toEqual(
      'other=param&tn=-1&f=-172800&t=1660038496&s=api_methods&qb=key1&qf=1-messages&qf=1-stats&qf=2%7EgetById&qf=2%7EgetLongPollServer&qf=2%7EgetSuggestedContacts&yl=118794&yh=140183&t1.s=api_methods&t1.qw=min&t1.qb=key1&t1.qf=0-production&t1.qf=2-add&t1.qf=2-getById&t1.yl=50&t1.yh=169&t2.s=api_methods&t2.qw=p90&t2.g=1&t2.qf=0-production&t2.qf=2-add&t2.qf=2-trackEvents&t2.n=10&t2.ts=-86400&t2.yl=131&t2.yh=161'
    );
  });
  test('change only tab', () => {
    const params = new URLSearchParams(
      '?f=-172800&t=1660038496&t3.yh=5800&t2.yh=161&tn=-1&t2.yl=131&t1.yl=50&t1.yh=169&s=api_methods&qb=key1&qf=key1-messages&qf=key1-stats&qf=key2%7EgetById&qf=key2%7EgetLongPollServer&qf=key2%7EgetSuggestedContacts&yl=118794.27083333333&yh=140183.15972222222&t1.s=api_methods&t1.qw=min&t1.qb=key1&t1.qf=key0-production&t1.qf=key2-add&t1.qf=key2-getById&t2.s=api_methods&t2.qw=p90&t2.g=1&t2.qf=key0-production&t2.qf=key2-add&t2.qf=key2-trackEvents&t2.n=10&t2.ts=-86400'
    );
    const value = readParams(
      { timeRange: { to: 1660038496, from: -172800 }, tagSync: [], plots: [], tabNum: 0 },
      params,
      defaultParams
    );

    const nextParams = writeParams(
      { timeRange: value.timeRange, tagSync: [], plots: value.plots, tabNum: 0 },
      new URLSearchParams('?other=param'),
      defaultParams
    );
    const nextValue = readParams(value, nextParams, defaultParams);
    expect(nextValue.plots === value.plots).toBe(true);
    expect(nextValue.tabNum).toBe(0);
  });
  test('change yLock for one graph', () => {
    const params = new URLSearchParams(
      '?f=-172800&t=1660038496&t3.yh=5800&t2.yh=161&tn=-1&t2.yl=131&t1.yl=50&t1.yh=169&s=api_methods&qb=key1&qf=key1-messages&qf=key1-stats&qf=key2%7EgetById&qf=key2%7EgetLongPollServer&qf=key2%7EgetSuggestedContacts&yl=118794.27083333333&yh=140183.15972222222&t1.s=api_methods&t1.qw=min&t1.qb=key1&t1.qf=key0-production&t1.qf=key2-add&t1.qf=key2-getById&t2.s=api_methods&t2.qw=p90&t2.g=1&t2.qf=key0-production&t2.qf=key2-add&t2.qf=key2-trackEvents&t2.n=10&t2.ts=-86400'
    );
    const value = readParams(
      { timeRange: { to: -1, from: 0 }, tagSync: [], plots: [], tabNum: 0 },
      params,
      defaultParams
    );

    const nextParams = writeParams(
      {
        timeRange: { ...value.timeRange },
        tagSync: [],
        plots: [value.plots[0], value.plots[1], { ...value.plots[2], yLock: { max: 10, min: 1 } }],
        tabNum: value.tabNum,
      },
      new URLSearchParams('?other=param'),
      defaultParams
    );
    const nextValue = readParams(value, nextParams, defaultParams);
    expect(nextValue.timeRange === value.timeRange).toBe(true);
    expect(nextValue.plots[0] === value.plots[0]).toBe(true);
    expect(nextValue.plots[1] === value.plots[1]).toBe(true);
    expect(nextValue.plots[2] !== value.plots[2]).toBe(true);
    expect(nextValue.plots[2].filterIn === value.plots[2].filterIn).toBe(true);
    expect(nextValue.plots[2].filterNotIn === value.plots[2].filterNotIn).toBe(true);
    expect(nextValue.plots[2].what === value.plots[2].what).toBe(true);
    expect(nextValue.plots[2].timeShifts === value.plots[2].timeShifts).toBe(true);
    expect(nextValue.tabNum === value.tabNum).toBe(true);
  });
});
