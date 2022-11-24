// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import './../testMock/matchMedia.mock';
import { convert } from './utils';

describe('utils', () => {
  test('convert hex', () => {
    expect(convert('hex', 0)).toBe('00000000');
    expect(convert('hex', 1)).toBe('00000001');
    expect(convert('hex', -1)).toBe('ffffffff');
    expect(convert('hex', -2147483648)).toBe('80000000');
    expect(convert('hex', 2147483647)).toBe('7fffffff');
    expect(convert('hex', -2142740666)).toBe('80485f46');
    expect(convert('hex', 167901850)).toBe('0a01fa9a');
  });
  test('convert hex_bswap', () => {
    expect(convert('hex_bswap', 0)).toBe('00000000');
    expect(convert('hex_bswap', 1)).toBe('01000000');
    expect(convert('hex_bswap', -1)).toBe('ffffffff');
    expect(convert('hex_bswap', -2147483648)).toBe('00000080');
    expect(convert('hex_bswap', 2147483647)).toBe('ffffff7f');
    expect(convert('hex_bswap', -2142740666)).toBe('465f4880');
    expect(convert('hex_bswap', 167901850)).toBe('9afa010a');
  });
  test('convert undefined', () => {
    expect(convert(undefined, 0)).toBe('0');
    expect(convert(undefined, 1)).toBe('1');
    expect(convert(undefined, -1)).toBe('-1');
    expect(convert(undefined, -2147483648)).toBe('-2147483648');
    expect(convert(undefined, 2147483647)).toBe('2147483647');
  });
  test('convert ip', () => {
    expect(convert('ip', 0)).toBe('0.0.0.0');
    expect(convert('ip', 1)).toBe('0.0.0.1');
    expect(convert('ip', -1)).toBe('255.255.255.255');
    expect(convert('ip', -2147483648)).toBe('128.0.0.0');
    expect(convert('ip', 2147483647)).toBe('127.255.255.255');
    expect(convert('ip', -2142740666)).toBe('128.72.95.70');
    expect(convert('ip', 2130730822)).toBe('127.0.95.70');
    expect(convert('ip', 167901850)).toBe('10.1.250.154');
  });
  test('convert ip_bswap', () => {
    expect(convert('ip_bswap', 0)).toBe('0.0.0.0');
    expect(convert('ip_bswap', 1)).toBe('1.0.0.0');
    expect(convert('ip_bswap', -1)).toBe('255.255.255.255');
    expect(convert('ip_bswap', -2147483648)).toBe('0.0.0.128');
    expect(convert('ip_bswap', 2147483647)).toBe('255.255.255.127');
    expect(convert('ip_bswap', -2142740666)).toBe('70.95.72.128');
    expect(convert('ip_bswap', 2130730822)).toBe('70.95.0.127');
    expect(convert('ip_bswap', 167901850)).toBe('154.250.1.10');
  });
  test('convert uint', () => {
    expect(convert('uint', 0)).toBe('0');
    expect(convert('uint', 1)).toBe('1');
    expect(convert('uint', -1)).toBe('4294967295');
    expect(convert('uint', -2147483648)).toBe('2147483648');
    expect(convert('uint', 2147483647)).toBe('2147483647');
    expect(convert('uint', -2142740666)).toBe('2152226630');
    expect(convert('uint', 2130730822)).toBe('2130730822');
  });

  test('convert timestamp', () => {
    expect(convert('timestamp', 0)).toBe('1970-01-01 00:00:00');
    expect(convert('timestamp', 1)).toBe('1970-01-01 00:00:01');
    expect(convert('timestamp', -1)).toBe('1969-12-31 23:59:59');
    expect(convert('timestamp', -2147483648)).toBe('1901-12-13 20:46:09');
    expect(convert('timestamp', 2147483647)).toBe('2038-01-19 03:14:07');
    expect(convert('timestamp', -2142740666)).toBe('1902-02-06 18:15:51');
    expect(convert('timestamp', 2130730822)).toBe('2037-07-09 05:40:22');
  });

  test('convert timestamp_local', () => {
    expect(convert('timestamp_local', 0)).toBe('1970-01-01 03:00:00');
    expect(convert('timestamp_local', 1)).toBe('1970-01-01 03:00:01');
    expect(convert('timestamp_local', -1)).toBe('1970-01-01 02:59:59');
    expect(convert('timestamp_local', -2147483648)).toBe('1901-12-13 23:16:09');
    expect(convert('timestamp_local', 2147483647)).toBe('2038-01-19 06:14:07');
    expect(convert('timestamp_local', -2142740666)).toBe('1902-02-06 20:45:51');
    expect(convert('timestamp_local', 2130730822)).toBe('2037-07-09 08:40:22');
  });
});
