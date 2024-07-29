import 'testMock/matchMedia.mock';
import { arrToObj } from './urlHelpers';
describe('urlHelpers', () => {
  test('arrToObj', () => {
    expect(
      arrToObj([
        ['a', '1'],
        ['a', '2'],
        ['b', '1'],
      ])
    ).toEqual({
      a: ['1', '2'],
      b: ['1'],
    });
  });
});
