import 'testMock/matchMedia.mock';
import { getDefaultParams } from '../getDefault';
import { getNewMetric } from './metric';
import { GET_PARAMS } from '../../api/enum';
import { toTreeObj } from '../urlHelpers';
import { orderPlotSplitter, removeValueChar } from '../constants';
import { widgetsParamsDecode } from './widgetsParamsDecode';
describe('urlStore widgetsParamsDecode', () => {
  test('widgetsParamsDecode', () => {
    const dParams = {
      ...getDefaultParams(),
      plots: {
        '0': {
          ...getNewMetric(),
          id: '0',
        },
        '2': {
          ...getNewMetric(),
          id: '2',
        },
      },
      orderPlot: ['2', '0'],
    };
    expect(widgetsParamsDecode({}, ['0', '2'], dParams)).toEqual({
      orderPlot: ['2', '0'],
      plots: {
        ...dParams.plots,
      },
    });
    expect(widgetsParamsDecode({ [GET_PARAMS.orderPlot]: {} }, ['0', '2'], dParams)).toEqual({
      orderPlot: ['2', '0'],
      plots: {
        ...dParams.plots,
      },
    });
    expect(widgetsParamsDecode(toTreeObj({ [GET_PARAMS.orderPlot]: [] }), ['0', '2'], dParams)).toEqual({
      orderPlot: ['2', '0'],
      plots: {
        ...dParams.plots,
      },
    });
    expect(
      widgetsParamsDecode(
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
      widgetsParamsDecode(toTreeObj({ [GET_PARAMS.plotPrefix + '0']: [removeValueChar] }), ['0', '2'], dParams)
    ).toEqual({
      orderPlot: ['2'],
      plots: {
        '2': dParams.plots['2'],
      },
    });

    expect(
      widgetsParamsDecode(toTreeObj({ [GET_PARAMS.plotPrefix + '2']: [removeValueChar] }), ['0', '2'], dParams)
    ).toEqual({
      orderPlot: ['0'],
      plots: {
        '0': dParams.plots['0'],
      },
    });
  });
});
