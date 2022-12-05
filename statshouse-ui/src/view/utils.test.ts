// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import './../testMock/matchMedia.mock';
import { convert, lexDecode } from './utils';

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

  test('test lexDecode', () => {
    // const min = -2_139_095_040;
    // const s1 = -8388608; // 0
    // const s2 = 8388607; // 0
    // const max = 2_139_095_039;

    expect(lexDecode(-2_139_095_041)).toBe(Number.NEGATIVE_INFINITY);
    expect(lexDecode(0)).toBe(0);
    expect(lexDecode(2_139_095_040)).toBe(Number.POSITIVE_INFINITY);
    expect(lexDecode(Number.NEGATIVE_INFINITY)).toBeNaN();

    let res0 = lexDecode(-2_139_095_040);
    for (let i = -2_139_095_039; i < -2_139_094_939; i++) {
      const res1 = lexDecode(i);
      expect(res1).toBeGreaterThan(res0);
      res0 = res1;
    }
    res0 = lexDecode(-1_139_095_040);
    for (let i = -1_139_095_039; i < -1_139_094_939; i++) {
      const res1 = lexDecode(i);
      expect(res1).toBeGreaterThan(res0);
      res0 = res1;
    }
    res0 = lexDecode(-8388709);
    for (let i = -8388708; i < -8388608; i++) {
      const res1 = lexDecode(i);
      expect(res1).toBeGreaterThan(res0);
      res0 = res1;
    }

    res0 = lexDecode(8388608);
    for (let i = 8388609; i < 8388709; i++) {
      const res1 = lexDecode(i);
      expect(res1).toBeGreaterThan(res0);
      res0 = res1;
    }

    res0 = lexDecode(1_139_094_938);
    for (let i = 1_139_094_939; i < 1_139_095_040; i++) {
      const res1 = lexDecode(i);
      expect(res1).toBeGreaterThan(res0);
      res0 = res1;
    }

    res0 = lexDecode(2_139_094_938);
    for (let i = 2_139_094_939; i < 2_139_095_040; i++) {
      const res1 = lexDecode(i);
      expect(res1).toBeGreaterThan(res0);
      res0 = res1;
    }
  });
  test('convert lexenc_float', () => {
    expect(convert('lexenc_float', 0)).toBe('0');
    expect(convert('lexenc_float', Number.NEGATIVE_INFINITY)).toBe('NaN');
    expect(convert('lexenc_float', -1_139_095_039)).toBe('-458.42181396484375');
    expect(convert('lexenc_float', 1_139_094_939)).toBe('458.4187927246094');
    expect(convert('lexenc_float', -8388709)).toBe('-1.1755083638069308e-38');
    expect(convert('lexenc_float', 8388609)).toBe('1.175494490952134e-38');
  });
});
