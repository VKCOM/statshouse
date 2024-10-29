import 'testMock/matchMedia.mock';
import { getNewMetric } from './getNewMetric';
import { metricFilterDecode } from './metricFilterDecode';
import {
  GET_PARAMS,
  METRIC_TYPE,
  METRIC_TYPE_URL,
  METRIC_VALUE_BACKEND_VERSION,
  PLOT_TYPE,
  QUERY_WHAT,
} from '../../../api/enum';
import { toTreeObj, treeParamsObjectValueSymbol } from '../../urlHelpers';
import { filterInSep, filterNotInSep, promQLMetric, removeValueChar } from '../../constants';
import { metricDecode } from './metricDecode';

describe('urlStore widgetsParams/metric/metricDecode.ts', () => {
  test('metricDecode', () => {
    const dParams = {
      ...getNewMetric(),
      id: '0',
    };
    expect(metricDecode('0')).toEqual({ ...dParams });
    expect(metricDecode('0', { [treeParamsObjectValueSymbol]: [removeValueChar] })).toEqual(undefined);
    expect(metricDecode('0', toTreeObj({}), dParams)).toEqual({ ...dParams });
    expect(
      metricDecode(
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
          [GET_PARAMS.version]: [METRIC_VALUE_BACKEND_VERSION.v1],
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
      backendVersion: METRIC_VALUE_BACKEND_VERSION.v1,
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
      metricDecode(
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
      metricDecode(
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
      metricDecode(
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
});
