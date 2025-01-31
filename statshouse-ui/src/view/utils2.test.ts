// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import './../testMock/matchMedia.mock';

import { convert, lexDecode, parseRawToInt } from './utils2';

jest.useFakeTimers().setSystemTime(new Date('2020-01-01 00:00:00'));

describe('utils', () => {
  test('convert hex', () => {
    expect(convert('hex', 0)).toBe('0x00000000');
    expect(convert('hex', 1)).toBe('0x00000001');
    expect(convert('hex', -1)).toBe('0xffffffff');
    expect(convert('hex', -2147483648)).toBe('0x80000000');
    expect(convert('hex', 2147483647)).toBe('0x7fffffff');
    expect(convert('hex', -2142740666)).toBe('0x80485f46');
    expect(convert('hex', 167901850)).toBe('0x0a01fa9a');
  });

  test('convert hex_bswap', () => {
    expect(convert('hex_bswap', 0)).toBe('0x00000000');
    expect(convert('hex_bswap', 1)).toBe('0x01000000');
    expect(convert('hex_bswap', -1)).toBe('0xffffffff');
    expect(convert('hex_bswap', -2147483648)).toBe('0x00000080');
    expect(convert('hex_bswap', 2147483647)).toBe('0xffffff7f');
    expect(convert('hex_bswap', -2142740666)).toBe('0x465f4880');
    expect(convert('hex_bswap', 167901850)).toBe('0x9afa010a');
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
    expect(convert('timestamp', 2147483647)).toBe('2038-01-19 03:14:07');
    expect(convert('timestamp', 2130730822)).toBe('2037-07-09 05:40:22');
    expect(convert('timestamp', 4104803674)).toBe('2100-01-28 07:14:34');
  });

  test('convert timestamp_local', () => {
    expect(convert('timestamp_local', 0)).toBe('1970-01-01 03:00:00');
    expect(convert('timestamp_local', 1)).toBe('1970-01-01 03:00:01');
    expect(convert('timestamp_local', 2147483647)).toBe('2038-01-19 06:14:07');
    expect(convert('timestamp_local', 2130730822)).toBe('2037-07-09 08:40:22');
    expect(convert('timestamp_local', 4104803674)).toBe('2100-01-28 10:14:34');
  });

  test('test lexDecode', () => {
    // const min = -2_139_095_040;
    // const s1 = -8388608; // 0
    // const s2 = 8388607; // 0
    // const max = 2_139_095_039;

    expect(lexDecode(-2_139_095_041)).toBe(Number.NEGATIVE_INFINITY);
    expect(lexDecode(0)).toBe(0);
    expect(lexDecode(-0)).toBe(0);
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
    expect(convert('lexenc_float', +0.0)).toBe('0');
    expect(convert('lexenc_float', -0.0)).toBe('0');
    expect(convert('lexenc_float', Number.NEGATIVE_INFINITY)).toBe('NaN');
    expect(convert('lexenc_float', -1_139_095_039)).toBe('-458.42181'); // -458.42181396484375
    expect(convert('lexenc_float', 1_139_094_939)).toBe('458.41879'); // 458.4187927246094
    expect(convert('lexenc_float', -8388709)).toBe('-1.1755084e-38'); // -1.1755083638069308e-38
    expect(convert('lexenc_float', 8388609)).toBe('1.1754945e-38'); // 1.175494490952134e-38
  });

  test('convert float', () => {
    expect(convert('float', 0)).toBe('0');
    expect(convert('float', -0.0)).toBe('0');
    expect(convert('float', -1)).toBe('NaN');
    expect(convert('float', 1)).toBe('0');
    expect(convert('float', 3212836864)).toBe('-1'); //uint
    expect(convert('float', -1082130432)).toBe('-1');
    expect(convert('float', 1065353216)).toBe('1');
    expect(convert('float', Number.NEGATIVE_INFINITY)).toBe('0');
    expect(convert('float', -1_139_095_039)).toBe('-0.0094475755'); // -0.00944757554680109
    expect(convert('float', 1_139_094_939)).toBe('458.41879'); // 458.4187927246094
    expect(convert('float', -8388709)).toBe('-3.4028032e+38'); // -3.402803183975685e+38
    expect(convert('float', 8388609)).toBe('1.1754945e-38'); // 1.175494490952134e-38
  });

  //parse raw
  test('parseRawToInt hex', () => {
    expect(parseRawToInt('hex', '0x00000000')).toBe(0);
    expect(parseRawToInt('hex', '0x00000001')).toBe(1);
    expect(parseRawToInt('hex', '0xffffffff')).toBe(-1);
    expect(parseRawToInt('hex', '0x80000000')).toBe(-2147483648);
    expect(parseRawToInt('hex', '0x7fffffff')).toBe(2147483647);
    expect(parseRawToInt('hex', '0x80485f46')).toBe(-2142740666);
    expect(parseRawToInt('hex', '0x0a01fa9a')).toBe(167901850);
  });

  test('parseRawToInt hex_bswap', () => {
    expect(parseRawToInt('hex_bswap', '0x00000000')).toBe(0);
    expect(parseRawToInt('hex_bswap', '0x01000000')).toBe(1);
    expect(parseRawToInt('hex_bswap', '0xffffffff')).toBe(-1);
    expect(parseRawToInt('hex_bswap', '0x00000080')).toBe(-2147483648);
    expect(parseRawToInt('hex_bswap', '0xffffff7f')).toBe(2147483647);
    expect(parseRawToInt('hex_bswap', '0x465f4880')).toBe(-2142740666);
    expect(parseRawToInt('hex_bswap', '0x9afa010a')).toBe(167901850);
  });

  test('parseRawToInt undefined', () => {
    expect(parseRawToInt(undefined, '0')).toBe(0);
    expect(parseRawToInt(undefined, '1')).toBe(1);
    expect(parseRawToInt(undefined, '-1')).toBe(-1);
    expect(parseRawToInt(undefined, '-2147483648')).toBe(-2147483648);
    expect(parseRawToInt(undefined, '2147483647')).toBe(2147483647);
  });

  test('parseRawToInt ip', () => {
    expect(parseRawToInt('ip', '0.0.0.0')).toBe(0);
    expect(parseRawToInt('ip', '0.0.0.1')).toBe(1);
    expect(parseRawToInt('ip', '255.255.255.255')).toBe(-1);
    expect(parseRawToInt('ip', '128.0.0.0')).toBe(-2147483648);
    expect(parseRawToInt('ip', '127.255.255.255')).toBe(2147483647);
    expect(parseRawToInt('ip', '128.72.95.70')).toBe(-2142740666);
    expect(parseRawToInt('ip', '127.0.95.70')).toBe(2130730822);
    expect(parseRawToInt('ip', '10.1.250.154')).toBe(167901850);
  });

  test('parseRawToInt ip_bswap', () => {
    expect(parseRawToInt('ip_bswap', '0.0.0.0')).toBe(0);
    expect(parseRawToInt('ip_bswap', '1.0.0.0')).toBe(1);
    expect(parseRawToInt('ip_bswap', '255.255.255.255')).toBe(-1);
    expect(parseRawToInt('ip_bswap', '0.0.0.128')).toBe(-2147483648);
    expect(parseRawToInt('ip_bswap', '255.255.255.127')).toBe(2147483647);
    expect(parseRawToInt('ip_bswap', '70.95.72.128')).toBe(-2142740666);
    expect(parseRawToInt('ip_bswap', '70.95.0.127')).toBe(2130730822);
    expect(parseRawToInt('ip_bswap', '154.250.1.10')).toBe(167901850);
  });

  test('parseRawToInt uint', () => {
    expect(parseRawToInt('uint', '0')).toBe(0);
    expect(parseRawToInt('uint', '1')).toBe(1);
    expect(parseRawToInt('uint', '2147483648')).toBe(-2147483648);
    expect(parseRawToInt('uint', '2147483647')).toBe(2147483647);
    expect(parseRawToInt('uint', '2152226630')).toBe(-2142740666);
    expect(parseRawToInt('uint', '2130730822')).toBe(2130730822);
  });

  test('parseRawToInt timestamp', () => {
    expect(parseRawToInt('timestamp', '1970-01-01 00:00:00')).toBe(0);
    expect(parseRawToInt('timestamp', '1970-01-01 00:00:01')).toBe(1);
    expect(parseRawToInt('timestamp', '2038-01-19 03:14:07')).toBe(2147483647);
    expect(parseRawToInt('timestamp', '2037-07-09 05:40:22')).toBe(2130730822);
    expect(parseRawToInt('timestamp', '2100-01-28 07:14:34')).toBe(4104803674);
  });

  test('parseRawToInt timestamp_local', () => {
    expect(parseRawToInt('timestamp_local', '1970-01-01 03:00:00')).toBe(0);
    expect(parseRawToInt('timestamp_local', '1970-01-01 03:00:01')).toBe(1);
    expect(parseRawToInt('timestamp_local', '2038-01-19 06:14:07')).toBe(2147483647);
    expect(parseRawToInt('timestamp_local', '2037-07-09 08:40:22')).toBe(2130730822);
    expect(parseRawToInt('timestamp_local', '2100-01-28 10:14:34')).toBe(4104803674);
  });

  test('parseRawToInt lexenc_float', () => {
    expect(parseRawToInt('lexenc_float', '0')).toBe(0);
    expect(parseRawToInt('lexenc_float', '-0.0')).toBe(0);
    expect(parseRawToInt('lexenc_float', '+0.0')).toBe(0);
    expect(parseRawToInt('lexenc_float', 'NaN')).toBeNaN();
    expect(parseRawToInt('lexenc_float', '-458.42181')).toBe(-1_139_095_039);
    expect(parseRawToInt('lexenc_float', '458.41879')).toBe(1_139_094_939);
    expect(parseRawToInt('lexenc_float', '-1.1755084e-38')).toBe(-8388709);
    expect(parseRawToInt('lexenc_float', '1.1754945e-38')).toBe(8388609);
  });

  test('parseRawToInt float', () => {
    expect(parseRawToInt('float', '0')).toBe(0);
    expect(parseRawToInt('float', '+0.0')).toBe(0);
    expect(parseRawToInt('float', '-0.0')).toBe(0);
    expect(parseRawToInt('float', 'NaN')).toBeNaN();
    expect(parseRawToInt('float', '1.4012985e-45')).toBe(1); //non 32 bit
    expect(parseRawToInt('float', '-1')).toBe(-1082130432);
    expect(parseRawToInt('float', '1')).toBe(1065353216);
    expect(parseRawToInt('float', '-0.0094475755')).toBe(-1_139_095_039);
    expect(parseRawToInt('float', '458.41879')).toBe(1_139_094_939);
    expect(parseRawToInt('float', '-3.4028032e+38')).toBe(-8388709);
    expect(parseRawToInt('float', '1.1754945e-38')).toBe(8388609);
  });
});
