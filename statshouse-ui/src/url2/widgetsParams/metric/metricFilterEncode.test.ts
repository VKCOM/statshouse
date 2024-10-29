import 'testMock/matchMedia.mock';
import { metricFilterEncode } from './metricFilterEncode';
import { GET_PARAMS } from '../../../api/enum';
describe('urlStore widgetsParams/metric/metricFilterEncode.ts', () => {
  test('metricFilterEncode', () => {
    expect(metricFilterEncode(GET_PARAMS.plotPrefix + '1.', {}, {})).toEqual([]);
    expect(metricFilterEncode(GET_PARAMS.plotPrefix + '1.', { '0': ['val1'] }, { '1': ['val1'] })).toEqual([
      ['t1.qf', '0-val1'],
      ['t1.qf', '1~val1'],
    ]);
  });
});
