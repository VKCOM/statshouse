import 'testMock/matchMedia.mock';
import { QueryParams } from '../queryParams';
import { getNewMetric } from './metric';
import { widgetsParamsEncode } from './widgetsParamsEncode';
import { removeValueChar } from '../constants';
import { getDefaultParams } from '../getDefault';

const params: QueryParams = getDefaultParams();

describe('urlStore widgetsParamsEncode', () => {
  test('urlEncodePlots', () => {
    const params2: QueryParams = {
      ...params,
      plots: {
        '0': {
          ...getNewMetric(),
          id: '0',
        },
        '1': {
          ...getNewMetric(),
          id: '1',
        },
      },
      orderPlot: ['0', '1'],
    };
    expect(widgetsParamsEncode(params)).toEqual([]);
    expect(widgetsParamsEncode(params, params)).toEqual([]);
    expect(
      widgetsParamsEncode(
        {
          ...params2,
          plots: {
            '1': {
              ...getNewMetric(),
              id: '1',
            },
          },
          orderPlot: ['1'],
        },
        params2
      )
    ).toEqual([
      ['t0', removeValueChar],
      ['op', '1'],
    ]);
  });
});
