// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import '@/testMock/matchMedia.mock';
import {
  urlEncode,
  urlEncodeGlobalParam,
  urlEncodeGroup,
  urlEncodeGroups,
  urlEncodeTimeRange,
  urlEncodeVariable,
  urlEncodeVariables,
  urlEncodeVariableSource,
} from './urlEncode';
import { GroupInfo, QueryParams, VariableParams } from './queryParams';
import { GET_PARAMS, TIME_RANGE_KEYS_TO } from '@/api/enum';
import { getDefaultParams, getNewGroup, getNewVariable, getNewVariableSource } from './getDefault';
import { removeValueChar } from './constants';

jest.useFakeTimers().setSystemTime(new Date('2020-01-01 00:00:00'));

const params: QueryParams = getDefaultParams();

describe('@/urlStore urlEncode', () => {
  test('@/urlEncodeVariableSource', () => {
    const dParam = {
      ...getNewVariableSource(),
      id: '0',
    };
    const prefix = GET_PARAMS.variablePrefix + '0.' + GET_PARAMS.variableSourcePrefix + dParam.id + '.';
    expect(urlEncodeVariableSource(prefix, dParam)).toEqual([[prefix + GET_PARAMS.variableSourceMetricName, '']]);
    expect(urlEncodeVariableSource(prefix, dParam, dParam)).toEqual([]);
    expect(
      urlEncodeVariableSource(
        prefix,
        { ...dParam, metric: 'm1', tag: '1', filterIn: { '0': ['v1', 'v2'] }, filterNotIn: { '1': ['n1', 'ns2'] } },
        dParam
      )
    ).toEqual([
      [prefix + GET_PARAMS.variableSourceMetricName, 'm1'],
      [prefix + GET_PARAMS.variableSourceTag, '1'],
      [prefix + GET_PARAMS.variableSourceFilter, '0-v1'],
      [prefix + GET_PARAMS.variableSourceFilter, '0-v2'],
      [prefix + GET_PARAMS.variableSourceFilter, '1~n1'],
      [prefix + GET_PARAMS.variableSourceFilter, '1~ns2'],
    ]);
  });
  test('@/urlEncodeVariable', () => {
    const dParam: VariableParams = {
      ...getNewVariable(),
      id: '0',
    };
    const prefix = GET_PARAMS.variablePrefix + dParam.id + '.';

    expect(urlEncodeVariable(dParam)).toEqual([[prefix + GET_PARAMS.variableName, '']]);
    expect(urlEncodeVariable(dParam, dParam)).toEqual([]);

    const dParam2: VariableParams = {
      ...dParam,
      name: 'var1',
      description: 'desc1',
      groupBy: true,
      negative: true,
      values: ['v1', 'v2'],
      source: {
        '0': {
          id: '0',
          metric: 'm1',
          tag: '1',
          filterIn: { '0': ['v1', 'v2'] },
          filterNotIn: { '1': ['n1', 'ns2'] },
        },
        '1': {
          id: '1',
          metric: 'm2',
          tag: '2',
          filterIn: {},
          filterNotIn: {},
        },
      },
      sourceOrder: ['0', '1'],
      link: [
        ['0', '0'],
        ['1', '0'],
      ],
    };
    const prefix2 = GET_PARAMS.variablePrefix + dParam2.id + '.';
    expect(urlEncodeVariable(dParam2, dParam)).toEqual([
      [prefix2 + GET_PARAMS.variableName, dParam2.name],
      [prefix2 + GET_PARAMS.variableDescription, dParam2.description],
      [prefix2 + GET_PARAMS.variableLinkPlot, '0.0-1.0'],
      [prefix2 + GET_PARAMS.variableSourcePrefix + '0.' + GET_PARAMS.variableSourceMetricName, 'm1'],
      [prefix2 + GET_PARAMS.variableSourcePrefix + '0.' + GET_PARAMS.variableSourceTag, '1'],
      [prefix2 + GET_PARAMS.variableSourcePrefix + '0.' + GET_PARAMS.variableSourceFilter, '0-v1'],
      [prefix2 + GET_PARAMS.variableSourcePrefix + '0.' + GET_PARAMS.variableSourceFilter, '0-v2'],
      [prefix2 + GET_PARAMS.variableSourcePrefix + '0.' + GET_PARAMS.variableSourceFilter, '1~n1'],
      [prefix2 + GET_PARAMS.variableSourcePrefix + '0.' + GET_PARAMS.variableSourceFilter, '1~ns2'],
      [prefix2 + GET_PARAMS.variableSourcePrefix + '1.' + GET_PARAMS.variableSourceMetricName, 'm2'],
      [prefix2 + GET_PARAMS.variableSourcePrefix + '1.' + GET_PARAMS.variableSourceTag, '2'],
      [GET_PARAMS.variableValuePrefix + '.' + dParam2.name, 'v1'],
      [GET_PARAMS.variableValuePrefix + '.' + dParam2.name, 'v2'],
      [GET_PARAMS.variableValuePrefix + '.' + dParam2.name + '.' + GET_PARAMS.variableGroupBy, '1'],
      [GET_PARAMS.variableValuePrefix + '.' + dParam2.name + '.' + GET_PARAMS.variableNegative, '1'],
    ]);
    expect(
      urlEncodeVariable(
        {
          ...dParam2,
          values: [],
          groupBy: false,
          negative: false,
          source: { '1': dParam2.source['1'] },
          sourceOrder: ['1'],
        },
        dParam2
      )
    ).toEqual([
      [prefix2 + GET_PARAMS.variableSourcePrefix + '0', removeValueChar],
      [GET_PARAMS.variableValuePrefix + '.' + dParam2.name, removeValueChar],
      [GET_PARAMS.variableValuePrefix + '.' + dParam2.name + '.' + GET_PARAMS.variableGroupBy, '0'],
      [GET_PARAMS.variableValuePrefix + '.' + dParam2.name + '.' + GET_PARAMS.variableNegative, '0'],
    ]);
  });
  test('@/urlEncodeVariables', () => {
    const params2: QueryParams = {
      ...params,
      variables: {
        '0': {
          id: '0',
          name: 'var1',
          description: 'desc1',
          groupBy: true,
          negative: true,
          values: ['v1', 'v2'],
          source: {
            '0': {
              id: '0',
              metric: 'm1',
              tag: '1',
              filterIn: { '0': ['v1', 'v2'] },
              filterNotIn: { '1': ['n1', 'ns2'] },
            },
            '1': {
              id: '1',
              metric: 'm2',
              tag: '2',
              filterIn: {},
              filterNotIn: {},
            },
          },
          sourceOrder: ['0', '1'],
          link: [
            ['0', '0'],
            ['1', '0'],
          ],
        },
        '1': {
          id: '1',
          name: 'var2',
          description: 'desc2',
          groupBy: true,
          negative: true,
          values: ['v21', 'v22'],
          source: {
            '0': {
              id: '0',
              metric: 'm1',
              tag: '1',
              filterIn: { '0': ['v1', 'v2'] },
              filterNotIn: { '1': ['n1', 'ns2'] },
            },
            '1': {
              id: '1',
              metric: 'm2',
              tag: '2',
              filterIn: {},
              filterNotIn: {},
            },
          },
          sourceOrder: ['0', '1'],
          link: [
            ['0', '0'],
            ['1', '0'],
          ],
        },
      },
      orderVariables: ['1', '0'],
    };
    expect(urlEncodeVariables(params)).toEqual([]);
    expect(
      urlEncodeVariables(
        {
          ...params2,
          variables: {
            '1': params2.variables['1'],
          },
          orderVariables: ['1'],
        },
        params2
      )
    ).toEqual([
      ['v0', removeValueChar],
      ['ov', '1'],
    ]);
  });
  test('@/urlEncodeGroup', () => {
    const dParam: GroupInfo = {
      ...getNewGroup(),
      id: '0',
    };
    expect(urlEncodeGroup(dParam)).toEqual([['g0.t', '']]);
    expect(urlEncodeGroup(dParam, dParam)).toEqual([]);
    expect(
      urlEncodeGroup({ ...dParam, show: false, size: '4', count: 4, name: 'n', description: 'd' }, dParam)
    ).toEqual([
      ['g0.t', 'n'],
      ['g0.d', 'd'],
      ['g0.n', '4'],
      ['g0.s', '4'],
      ['g0.v', '0'],
    ]);
    expect(urlEncodeGroup({ ...dParam, show: true }, { ...dParam, show: false })).toEqual([['g0.v', '1']]);
  });
  test('@/urlEncodeGroups', () => {
    const params2: QueryParams = {
      ...params,
      groups: {
        '0': {
          ...getNewGroup(),
          id: '0',
        },
        '1': {
          ...getNewGroup(),
          id: '1',
        },
      },
      orderGroup: ['1', '0'],
    };
    expect(urlEncodeGroups(params)).toEqual([]);
    expect(urlEncodeGroups(params2, params2)).toEqual([]);
    expect(
      urlEncodeGroups(
        {
          ...params2,
          groups: {
            '1': params2.groups[1],
          },
          orderGroup: ['1'],
        },
        params2
      )
    ).toEqual([
      ['g0', removeValueChar],
      ['og', '1'],
    ]);
  });

  test('@/urlEncodeTimeRange', () => {
    expect(urlEncodeTimeRange(params)).toEqual([]);
    expect(urlEncodeTimeRange(params, params)).toEqual([]);
    expect(
      urlEncodeTimeRange(
        {
          ...params,
          timeRange: {
            ...params.timeRange,
            from: -1000,
            urlTo: TIME_RANGE_KEYS_TO.Now,
          },
        },
        params
      )
    ).toEqual([
      ['t', '0'],
      ['f', '-1000'],
    ]);
  });
  test('@/urlEncodeGlobalParam', () => {
    expect(urlEncodeGlobalParam(params)).toEqual([]);
    expect(urlEncodeGlobalParam(params, params)).toEqual([]);
    expect(
      urlEncodeGlobalParam(
        {
          ...params,
          dashboardId: '1',
          dashboardName: 'dn',
          dashboardDescription: 'dd',
          dashboardVersion: 123,
          dashboardCurrentVersion: 12345,
          eventFrom: 2000,
          live: true,
          tabNum: '5',
          theme: 'light',
          timeShifts: [2000, 4000],
        },
        { ...params }
      )
    ).toEqual([
      ['live', '1'],
      ['theme', 'light'],
      ['tn', '5'],
      ['ts', '2000'],
      ['ts', '4000'],
      ['ef', '2000'],
      ['id', '1'],
      ['dn', 'dn'],
      ['dd', 'dd'],
      ['dv', '123'],
    ]);
    expect(
      urlEncodeGlobalParam(
        {
          ...params,
          dashboardVersion: 123,
          dashboardCurrentVersion: 123,
        },
        { ...params }
      )
    ).toEqual([]);
    expect(
      urlEncodeGlobalParam(
        {
          ...params,
          live: false,
        },
        { ...params, live: true }
      )
    ).toEqual([['live', '0']]);
  });

  test('@/urlEncode', () => {
    expect(urlEncode(params)).toEqual([]);
  });
});
