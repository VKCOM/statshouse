import 'testMock/matchMedia.mock';
import { type QueryParams } from './queryParams';
import { urlEncode } from './urlEncode';
import { urlDecode } from './urlDecode';
import { getDefaultParams, getNewGroup, getNewVariable } from './getDefault';
import { arrToObj, toTreeObj } from './index';
import { getNewMetric } from './widgetsParams';

jest.useFakeTimers().setSystemTime(new Date('2020-01-01 00:00:00'));

const params: QueryParams = {
  ...getDefaultParams(),
  timeRange: { absolute: true, from: 0, now: 1577826000, to: 1577826000, urlTo: 1577826000 },
  plots: {
    '0': { ...getNewMetric(), id: '0' },
    '1': { ...getNewMetric(), id: '1' },
    '2': { ...getNewMetric(), id: '2' },
    '3': { ...getNewMetric(), id: '3' },
  },
  orderPlot: ['1', '0', '2', '3'],
  groups: {
    '0': { ...getNewGroup(), id: '0' },
    '1': { ...getNewGroup(), id: '1' },
    '2': { ...getNewGroup(), id: '2' },
    '3': { ...getNewGroup(), id: '3' },
  },
  orderGroup: ['3', '2', '1', '0'],
  variables: {
    '0': { ...getNewVariable(), id: '0' },
    '1': { ...getNewVariable(), id: '1' },
    '2': { ...getNewVariable(), id: '2' },
    '3': { ...getNewVariable(), id: '3' },
  },
  orderVariables: ['3', '2', '1', '0'],
};
const params2: QueryParams = {
  ...getDefaultParams(),
  timeRange: { absolute: true, from: 0, now: 1577826000, to: 1577826000, urlTo: 1577826000 },
  plots: {
    '1': { ...getNewMetric(), id: '1' },
    '2': { ...getNewMetric(), id: '2' },
    '3': { ...getNewMetric(), id: '3' },
  },
  orderPlot: ['1', '2', '3'],
  groups: {
    '0': { ...getNewGroup(), id: '0' },
    '2': { ...getNewGroup(), id: '2' },
    '3': { ...getNewGroup(), id: '3' },
  },
  orderGroup: ['3', '2', '0'],
  variables: {
    '0': { ...getNewVariable(), id: '0' },
    '1': { ...getNewVariable(), id: '1' },
    '3': { ...getNewVariable(), id: '3' },
  },
  orderVariables: ['3', '1', '0'],
};

describe('urlStore', () => {
  test.skip('urlEncode => urlDecode', () => {
    expect(urlDecode(toTreeObj(arrToObj(urlEncode(params))), params)).toEqual(params);
  });
  test('urlEncode => urlDecode save', () => {
    // expect(urlDecode(toTreeObj(arrToObj(urlEncode(params2, params))), params)).toEqual(params2);
    expect(urlDecode(toTreeObj(arrToObj(urlEncode(params, params2))), params2)).toEqual(params);
  });
});
