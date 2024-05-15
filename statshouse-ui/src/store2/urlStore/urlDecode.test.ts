import './../../testMock/matchMedia.mock';
import {
  urlDecode,
  urlDecodeGlobalParam,
  urlDecodeGroup,
  urlDecodeGroups,
  urlDecodePlot,
  urlDecodePlotFilters,
  urlDecodePlots,
  urlDecodeTimeRange,
  urlDecodeVariable,
  urlDecodeVariables,
  urlDecodeVariableSource,
} from './urlDecode';
import {
  filterInSep,
  filterNotInSep,
  getDefaultParams,
  getNewGroup,
  getNewPlot,
  getNewVariable,
  getNewVariableSource,
  orderGroupSplitter,
  orderPlotSplitter,
  orderVariableSplitter,
  promQLMetric,
  removeValueChar,
  toTreeObj,
  treeParamsObjectValueSymbol,
} from './lib';
import { GET_PARAMS, METRIC_TYPE, METRIC_TYPE_URL, PLOT_TYPE, QUERY_WHAT, TIME_RANGE_KEYS_TO } from '../../api/enum';
import { THEMES } from '../../store';
import { QueryParams, VariableParams, VariableParamsSource } from './queryParams';

jest.useFakeTimers().setSystemTime(new Date('2020-01-01 00:00:00'));

describe('urlStore urlDecode', () => {
  test('urlDecodeGlobalParam', () => {
    const dParams = {
      ...getDefaultParams(),
      dashboardDescription: 'desc',
      dashboardId: '1546',
      dashboardName: 'name',
      dashboardVersion: 123,
      eventFrom: 234,
      live: true,
      tabNum: '-1',
      theme: THEMES.Light,
      timeShifts: [24, 48],
    };
    const p = urlDecodeGlobalParam({}, dParams);
    expect(p).toEqual({
      dashboardDescription: 'desc',
      dashboardId: '1546',
      dashboardName: 'name',
      dashboardVersion: 123,
      eventFrom: 234,
      live: true,
      tabNum: '-1',
      theme: THEMES.Light,
      timeShifts: [24, 48],
    });

    const p2 = urlDecodeGlobalParam(
      toTreeObj({
        [GET_PARAMS.dashboardID]: ['6587'],
        [GET_PARAMS.dashboardName]: ['name2'],
        [GET_PARAMS.dashboardDescription]: [''],
        [GET_PARAMS.dashboardVersion]: ['321'],
        [GET_PARAMS.metricEventFrom]: ['432'],
        [GET_PARAMS.metricLive]: ['0'],
        [GET_PARAMS.metricTabNum]: ['1'],
        [GET_PARAMS.theme]: [THEMES.Dark],
        [GET_PARAMS.metricTimeShifts]: [removeValueChar],
      }),
      dParams
    );
    expect(p2).toEqual({
      dashboardDescription: '',
      dashboardId: '6587',
      dashboardName: 'name2',
      dashboardVersion: 321,
      eventFrom: 432,
      live: false,
      tabNum: '1',
      theme: THEMES.Dark,
      timeShifts: [],
    });
    const p3 = urlDecodeGlobalParam(
      {
        [GET_PARAMS.dashboardID]: {},
        [GET_PARAMS.dashboardName]: {},
        [GET_PARAMS.dashboardDescription]: {},
        [GET_PARAMS.dashboardVersion]: {},
        [GET_PARAMS.metricEventFrom]: {},
        [GET_PARAMS.metricLive]: {},
        [GET_PARAMS.metricTabNum]: {},
        [GET_PARAMS.theme]: {},
        [GET_PARAMS.metricTimeShifts]: {},
      },
      dParams
    );
    expect(p3).toEqual({
      dashboardDescription: 'desc',
      dashboardId: '1546',
      dashboardName: 'name',
      dashboardVersion: 123,
      eventFrom: 234,
      live: true,
      tabNum: '-1',
      theme: THEMES.Light,
      timeShifts: [24, 48],
    });
  });
  test('urlDecodeTimeRange', () => {
    const dParams = {
      ...getDefaultParams(),
      timeRange: {
        absolute: true,
        from: -400,
        now: 1577826000,
        to: 1577826000,
        urlTo: TIME_RANGE_KEYS_TO.default,
      },
    };

    const p = urlDecodeTimeRange({}, dParams);
    expect(p).toEqual({
      timeRange: {
        absolute: true,
        from: -400,
        now: 1577826000,
        to: 1577826000,
        urlTo: 1577826000,
      },
    });

    const p2 = urlDecodeTimeRange(
      toTreeObj({ [GET_PARAMS.toTime]: ['1577825000'], [GET_PARAMS.fromTime]: ['-2000'] }),
      dParams
    );
    expect(p2).toEqual({
      timeRange: {
        absolute: true,
        from: -2000,
        now: 1577826000,
        to: 1577825000,
        urlTo: 1577825000,
      },
    });

    const p3 = urlDecodeTimeRange({ [GET_PARAMS.toTime]: {}, [GET_PARAMS.fromTime]: {} }, dParams);
    expect(p3).toEqual({
      timeRange: {
        absolute: true,
        from: -400,
        now: 1577826000,
        to: 1577826000,
        urlTo: 1577826000,
      },
    });
  });
  test('urlDecodePlotFilters', () => {
    const dParams = {
      ...getNewPlot(),
      filterIn: {},
      filterNotIn: {},
    };
    expect(urlDecodePlotFilters(GET_PARAMS.metricFilter, {}, dParams)).toEqual({ filterIn: {}, filterNotIn: {} });
    expect(urlDecodePlotFilters(GET_PARAMS.metricFilter, {})).toEqual({ filterIn: {}, filterNotIn: {} });
    expect(urlDecodePlotFilters(GET_PARAMS.metricFilter)).toEqual({ filterIn: {}, filterNotIn: {} });
    expect(urlDecodePlotFilters(GET_PARAMS.metricFilter, { [GET_PARAMS.metricFilter]: {} }, dParams)).toEqual({
      filterIn: {},
      filterNotIn: {},
    });
    expect(
      urlDecodePlotFilters(
        GET_PARAMS.metricFilter,
        toTreeObj({
          [GET_PARAMS.metricFilter]: ['0' + filterInSep + 'tag-value', '1' + filterNotInSep + 'tag-value'],
        }),
        dParams
      )
    ).toEqual({ filterIn: { '0': ['tag-value'] }, filterNotIn: { '1': ['tag-value'] } });
    expect(
      urlDecodePlotFilters(
        GET_PARAMS.metricFilter,
        toTreeObj({
          [GET_PARAMS.metricFilter]: [
            '1' + filterNotInSep + 'tag4' + filterInSep + 'value',
            '1' + filterNotInSep + 'tag2' + filterInSep + 'value',
            '0' + filterInSep + 'tag4' + filterNotInSep + 'value',
            '0' + filterInSep + 'tag3' + filterNotInSep + 'value',
          ],
        }),
        dParams
      )
    ).toEqual({
      filterIn: { '0': ['tag3' + filterNotInSep + 'value', 'tag4' + filterNotInSep + 'value'] },
      filterNotIn: { '1': ['tag2' + filterInSep + 'value', 'tag4' + filterInSep + 'value'] },
    });
  });
  test('urlDecodePlot', () => {
    const dParams = {
      ...getNewPlot(),
      id: '0',
    };
    expect(urlDecodePlot('0')).toEqual({ ...dParams });
    expect(urlDecodePlot('0', { [treeParamsObjectValueSymbol]: [removeValueChar] })).toEqual(undefined);
    expect(urlDecodePlot('0', toTreeObj({}), dParams)).toEqual({ ...dParams });
    expect(
      urlDecodePlot(
        '0',
        toTreeObj({
          [GET_PARAMS.metricName]: ['metric1'],
          [GET_PARAMS.metricCustomName]: ['metric1custom'],
          [GET_PARAMS.metricCustomDescription]: ['metric1customDesc'],
          [GET_PARAMS.metricPromQL]: ['promql'],
          [GET_PARAMS.metricMetricUnit]: [METRIC_TYPE_URL.byte],
          [GET_PARAMS.metricWhat]: [QUERY_WHAT.count],
          [GET_PARAMS.metricAgg]: ['5'],
          [GET_PARAMS.metricGroupBy]: ['5', '2key', 'key2', '_s', '12', '4'],
          [GET_PARAMS.metricFilter]: [],
          [GET_PARAMS.numResults]: ['10'],
          [GET_PARAMS.version]: ['1'],
          [GET_PARAMS.metricLockMin]: ['-100'],
          [GET_PARAMS.metricLockMax]: ['200'],
          [GET_PARAMS.metricMaxHost]: ['1'],
          [GET_PARAMS.metricType]: [PLOT_TYPE.Event],
          [GET_PARAMS.metricEvent]: ['5', '2key', '2', '12', '4'],
          [GET_PARAMS.metricEventBy]: ['5', '2key', 'key2', '_s', '12', '4'],
          [GET_PARAMS.metricEventHide]: ['5', '2key', 'key2', '_s', '12', '4'],
          [GET_PARAMS.viewTotalLine]: ['1'],
          [GET_PARAMS.viewFilledGraph]: ['0'],
          [GET_PARAMS.metricLocalTimeShifts]: ['200', '100'],
        }),
        dParams
      )
    ).toEqual({
      ...dParams,
      id: '0',
      metricName: 'metric1',
      customName: 'metric1custom',
      customDescription: 'metric1customDesc',
      promQL: 'promql',
      metricUnit: METRIC_TYPE.byte,
      what: [QUERY_WHAT.count],
      customAgg: 5,
      groupBy: ['2', '4', '5', '12', '_s'],
      filterIn: {},
      filterNotIn: {},
      numSeries: 10,
      useV2: false,
      yLock: {
        min: -100,
        max: 200,
      },
      maxHost: true,
      type: PLOT_TYPE.Event,
      events: ['2', '4', '5', '12'],
      eventsBy: ['2', '4', '5', '12', '_s'],
      eventsHide: ['2', '4', '5', '12', '_s'],
      totalLine: true,
      filledGraph: false,
      timeShifts: [100, 200],
    });
    expect(
      urlDecodePlot(
        '0',
        {
          [GET_PARAMS.metricName]: {},
          [GET_PARAMS.metricCustomName]: {},
          [GET_PARAMS.metricCustomDescription]: {},
          [GET_PARAMS.metricPromQL]: {},
          [GET_PARAMS.metricMetricUnit]: {},
          [GET_PARAMS.metricWhat]: {},
          [GET_PARAMS.metricAgg]: {},
          [GET_PARAMS.metricGroupBy]: {},
          [GET_PARAMS.metricFilter]: {},
          [GET_PARAMS.numResults]: {},
          [GET_PARAMS.version]: {},
          [GET_PARAMS.metricLockMin]: {},
          [GET_PARAMS.metricLockMax]: {},
          [GET_PARAMS.metricMaxHost]: {},
          [GET_PARAMS.metricType]: {},
          [GET_PARAMS.metricEvent]: {},
          [GET_PARAMS.metricEventBy]: {},
          [GET_PARAMS.metricEventHide]: {},
          [GET_PARAMS.viewTotalLine]: {},
          [GET_PARAMS.viewFilledGraph]: {},
          [GET_PARAMS.metricLocalTimeShifts]: {},
        },
        dParams
      )
    ).toEqual({
      ...dParams,
    });

    expect(
      urlDecodePlot(
        '0',
        toTreeObj({
          [GET_PARAMS.metricName]: [''],
          // [GET_PARAMS.metricPromQL]: [''],
        }),
        dParams
      )
    ).toEqual({
      ...dParams,
    });
    expect(
      urlDecodePlot(
        '0',
        toTreeObj({
          // [GET_PARAMS.metricName]: [''],
          [GET_PARAMS.metricPromQL]: [''],
        }),
        dParams
      )
    ).toEqual({
      ...dParams,
      metricName: promQLMetric,
    });
  });
  test('urlDecodePlots', () => {
    const dParams = {
      ...getDefaultParams(),
      plots: {
        '0': {
          ...getNewPlot(),
          id: '0',
        },
        '2': {
          ...getNewPlot(),
          id: '2',
        },
      },
      orderPlot: ['2', '0'],
    };
    expect(urlDecodePlots({}, ['0', '2'], dParams)).toEqual({
      orderPlot: ['2', '0'],
      plots: {
        ...dParams.plots,
      },
    });
    expect(urlDecodePlots({ [GET_PARAMS.orderPlot]: {} }, ['0', '2'], dParams)).toEqual({
      orderPlot: ['2', '0'],
      plots: {
        ...dParams.plots,
      },
    });
    expect(urlDecodePlots(toTreeObj({ [GET_PARAMS.orderPlot]: [] }), ['0', '2'], dParams)).toEqual({
      orderPlot: ['2', '0'],
      plots: {
        ...dParams.plots,
      },
    });
    expect(
      urlDecodePlots(
        toTreeObj({ [GET_PARAMS.orderPlot]: [['2', '0', '3er'].join(orderPlotSplitter)] }),
        ['0', '2'],
        dParams
      )
    ).toEqual({
      orderPlot: ['2', '0'],
      plots: {
        ...dParams.plots,
      },
    });
    expect(
      urlDecodePlots(toTreeObj({ [GET_PARAMS.plotPrefix + '0']: [removeValueChar] }), ['0', '2'], dParams)
    ).toEqual({
      orderPlot: ['2'],
      plots: {
        '2': dParams.plots['2'],
      },
    });

    expect(
      urlDecodePlots(toTreeObj({ [GET_PARAMS.plotPrefix + '2']: [removeValueChar] }), ['0', '2'], dParams)
    ).toEqual({
      orderPlot: ['0'],
      plots: {
        '0': dParams.plots['0'],
      },
    });
  });
  test('urlDecodeGroup', () => {
    const dParams = {
      ...getNewGroup(),
      id: '0',
    };
    expect(urlDecodeGroup('0', undefined, dParams)).toEqual(dParams);
    expect(urlDecodeGroup('0', {}, dParams)).toEqual(dParams);
    expect(urlDecodeGroup('0', { [treeParamsObjectValueSymbol]: [removeValueChar] }, dParams)).toEqual(undefined);
    expect(
      urlDecodeGroup(
        '0',
        {
          [GET_PARAMS.dashboardGroupInfoName]: {},
          [GET_PARAMS.dashboardGroupInfoDescription]: {},
          [GET_PARAMS.dashboardGroupInfoShow]: {},
          [GET_PARAMS.dashboardGroupInfoCount]: {},
          [GET_PARAMS.dashboardGroupInfoSize]: {},
        },
        dParams
      )
    ).toEqual(dParams);
    expect(
      urlDecodeGroup(
        '0',
        toTreeObj({
          [GET_PARAMS.dashboardGroupInfoName]: ['name'],
          [GET_PARAMS.dashboardGroupInfoDescription]: ['desc'],
          [GET_PARAMS.dashboardGroupInfoShow]: ['0'],
          [GET_PARAMS.dashboardGroupInfoCount]: ['2'],
          [GET_PARAMS.dashboardGroupInfoSize]: ['4'],
        }),
        dParams
      )
    ).toEqual({
      count: 2,
      description: 'desc',
      id: '0',
      name: 'name',
      show: false,
      size: '4',
    });
  });
  test('urlDecodeGroups', () => {
    const dParams = {
      ...getDefaultParams(),
      groups: {
        '0': { ...getNewGroup(), id: '0' },
        '2': { ...getNewGroup(), id: '2' },
      },
      orderGroup: ['2', '0'],
    };
    const resParams = {
      groups: dParams.groups,
      orderGroup: dParams.orderGroup,
    };
    expect(urlDecodeGroups({}, ['0', '2'], dParams)).toEqual(resParams);
    expect(urlDecodeGroups({ [GET_PARAMS.orderGroup]: {} }, ['0', '2'], dParams)).toEqual(resParams);
    expect(
      urlDecodeGroups(
        toTreeObj({
          [GET_PARAMS.orderGroup]: [],
        }),
        ['0', '2'],
        dParams
      )
    ).toEqual(resParams);
    expect(
      urlDecodeGroups(
        toTreeObj({
          [GET_PARAMS.orderGroup]: [['0,2'].join(orderGroupSplitter)],
          [GET_PARAMS.dashboardGroupInfoPrefix + '0']: [removeValueChar],
        }),
        ['0', '2'],
        dParams
      )
    ).toEqual({ orderGroup: ['2'], groups: { '2': dParams.groups['2'] } });
  });
  test('urlDecodeVariableSource', () => {
    const dParams: VariableParamsSource = { ...getNewVariableSource(), id: '0' };
    expect(urlDecodeVariableSource(dParams.id, {}, dParams)).toEqual(dParams);
    expect(
      urlDecodeVariableSource(
        dParams.id,
        {
          [GET_PARAMS.variableSourceMetricName]: {},
          [GET_PARAMS.variableSourceTag]: {},
          [GET_PARAMS.variableSourceFilter]: {},
        },
        dParams
      )
    ).toEqual(dParams);
    expect(
      urlDecodeVariableSource(
        dParams.id,
        toTreeObj({
          [GET_PARAMS.variableSourceMetricName]: ['metric'],
          [GET_PARAMS.variableSourceTag]: ['_s'],
          [GET_PARAMS.variableSourceFilter]: [],
        }),
        dParams
      )
    ).toEqual({ ...dParams, metric: 'metric', tag: '_s' });
  });
  test('urlDecodeVariable', () => {
    const dParams: VariableParams = {
      ...getNewVariable(),
      id: '0',
      name: 'name',
      source: {
        '0': { ...getNewVariableSource(), id: '0' },
        '2': { ...getNewVariableSource(), id: '2' },
      },
      link: [
        ['0', '0'],
        ['1', '0'],
      ],
      sourceOrder: ['0', '2'],
      values: ['v1', 'v2'],
    };
    expect(urlDecodeVariable(dParams.id, undefined, undefined, dParams)).toEqual(dParams);
    expect(urlDecodeVariable(dParams.id, { [treeParamsObjectValueSymbol]: [removeValueChar] }, {}, dParams)).toEqual(
      undefined
    );
    expect(urlDecodeVariable(dParams.id, {}, {}, dParams)).toEqual(dParams);
    expect(
      urlDecodeVariable(
        dParams.id,
        { [GET_PARAMS.variableName]: {}, [GET_PARAMS.variableDescription]: {}, [GET_PARAMS.variableLinkPlot]: {} },
        {},
        dParams
      )
    ).toEqual(dParams);
    expect(
      urlDecodeVariable(
        dParams.id,
        toTreeObj({
          [GET_PARAMS.variableName]: ['name'],
          [GET_PARAMS.variableDescription]: ['desc'],
          [GET_PARAMS.variableSourcePrefix + '0.' + GET_PARAMS.variableSourceMetricName]: [''],
          [GET_PARAMS.variableSourcePrefix + '2err.' + GET_PARAMS.variableSourceMetricName]: [''],
          [GET_PARAMS.variableSourcePrefix + '2']: [removeValueChar],
          [GET_PARAMS.variableLinkPlot]: ['0.0-1.1'],
        }),
        {},
        dParams
      )
    ).toEqual({
      ...dParams,
      description: 'desc',
      source: {
        '0': dParams.source['0'],
      },
      sourceOrder: ['0'],
      link: [
        ['0', '0'],
        ['1', '1'],
      ],
    });

    expect(
      urlDecodeVariable(
        dParams.id,
        {},
        {
          [dParams.name]: {
            [GET_PARAMS.variableNegative]: {},
            [GET_PARAMS.variableGroupBy]: {},
          },
        },
        dParams
      )
    ).toEqual(dParams);
    expect(
      urlDecodeVariable(
        dParams.id,
        {},
        {
          [dParams.name]: {
            [treeParamsObjectValueSymbol]: ['v1'],
            [GET_PARAMS.variableNegative]: { [treeParamsObjectValueSymbol]: ['1'] },
            [GET_PARAMS.variableGroupBy]: { [treeParamsObjectValueSymbol]: ['1'] },
          },
        },
        dParams
      )
    ).toEqual({ ...dParams, negative: true, groupBy: true, values: ['v1'] });
    expect(
      urlDecodeVariable(
        dParams.id,
        toTreeObj({
          [GET_PARAMS.variableLinkPlot]: [],
        }),
        toTreeObj({
          [dParams.name]: [removeValueChar],
        }),
        dParams
      )
    ).toEqual({ ...dParams, values: [] });
  });
  test('urlDecodeVariables', () => {
    const dParams: QueryParams = {
      ...getDefaultParams(),
      variables: {
        '0': { ...getNewVariable(), id: '0' },
        '2': { ...getNewVariable(), id: '2' },
      },
      orderVariables: ['2', '0'],
    };
    const dVariable = {
      variables: dParams.variables,
      orderVariables: dParams.orderVariables,
    };

    expect(urlDecodeVariables({}, ['0', '2'], dParams)).toEqual(dVariable);
    expect(urlDecodeVariables({ [GET_PARAMS.orderVariable]: {} }, ['0', '2'], dParams)).toEqual(dVariable);
    expect(
      urlDecodeVariables(
        toTreeObj({
          [GET_PARAMS.orderVariable]: [['0', '2'].join(orderVariableSplitter)],
          [GET_PARAMS.variablePrefix + '0']: [removeValueChar],
        }),
        ['0', '2'],
        dParams
      )
    ).toEqual({
      ...dVariable,
      variables: {
        '2': dVariable.variables['2'],
      },
      orderVariables: ['2'],
    });
  });
  test('urlDecode', () => {
    const dParams: QueryParams = {
      ...getDefaultParams(),
      timeRange: {
        absolute: true,
        from: 0,
        now: 1577826000,
        to: 1577826000,
        urlTo: 1577826000,
      },
      plots: {
        '0': {
          ...getNewPlot(),
          id: '0',
        },
        '1': {
          ...getNewPlot(),
          id: '1',
        },
        '2': {
          ...getNewPlot(),
          id: '2',
        },
      },
      orderPlot: ['0', '1', '2'],
      variables: {
        '0': {
          ...getNewVariable(),
          id: '0',
        },
        '1': {
          ...getNewVariable(),
          id: '1',
        },
      },
      orderVariables: ['0', '1'],
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
      orderGroup: ['0', '1'],
    };
    expect(urlDecode({}, dParams)).toEqual(dParams);
    expect(
      urlDecode(
        toTreeObj({
          [GET_PARAMS.metricName]: ['m0'],
          [GET_PARAMS.plotPrefix + '1.' + GET_PARAMS.metricName]: ['m1'],
          [GET_PARAMS.plotPrefix + '2.' + GET_PARAMS.metricPromQL]: ['q2'],
          [GET_PARAMS.variablePrefix + '0.' + GET_PARAMS.variableName]: ['v0'],
          [GET_PARAMS.variablePrefix + '1.' + GET_PARAMS.variableName]: ['v1'],
          [GET_PARAMS.dashboardGroupInfoPrefix + '0.' + GET_PARAMS.dashboardGroupInfoName]: ['g0'],
          [GET_PARAMS.dashboardGroupInfoPrefix + '1.' + GET_PARAMS.dashboardGroupInfoName]: ['g1'],
        }),
        dParams
      )
    ).toEqual({
      ...dParams,
      plots: {
        '0': { ...dParams.plots['0'], metricName: 'm0' },
        '1': { ...dParams.plots['1'], metricName: 'm1' },
        '2': { ...dParams.plots['2'], metricName: promQLMetric, promQL: 'q2' },
      },
      variables: {
        '0': { ...dParams.variables['0'], name: 'v0' },
        '1': { ...dParams.variables['1'], name: 'v1' },
      },
      groups: {
        '0': { ...dParams.groups['0'], name: 'g0' },
        '1': { ...dParams.groups['1'], name: 'g1' },
      },
    });
    expect(
      urlDecode(
        toTreeObj({
          [GET_PARAMS.metricPromQL]: ['q0'],
        }),
        dParams
      )
    ).toEqual({
      ...dParams,
      plots: {
        ...dParams.plots,
        '0': { ...dParams.plots['0'], metricName: promQLMetric, promQL: 'q0' },
      },
    });
  });
});
