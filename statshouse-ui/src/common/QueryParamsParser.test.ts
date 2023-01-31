// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import {
  BooleanParam,
  ConfigParams,
  decodeQueryParams,
  encodeQueryParams,
  FilterParams,
  mergeLeft,
  NumberParam,
  ObjectsParam,
  TagSyncParam,
  TimeToParam,
  UseV2Param,
} from './QueryParamsParser';
import { KeysTo, TIME_RANGE_KEYS_TO } from './TimeRange';
import { queryValueBackendVersion1, queryValueBackendVersion2 } from '../view/api';

let locationSearch = '';
// window.location search mock
Object.defineProperty(window, 'location', {
  configurable: true,
  get() {
    return { search: locationSearch };
  },
});

type Value = {
  str: string;
  num: number;
  arr: number[];
  arr2: number[];
  arrPrefix: { a: string }[];
  arrPrefix2: { a2: string; b2: string }[];
  arrPrefix_not_default?: { and: string }[];
  zero: number;
  undef?: string;
  not_default?: string | null;
  time: {
    to: number | KeysTo;
    to2: number | KeysTo;
  };
  obj: Record<
    string,
    {
      a: number;
      b: string;
    }
  >;
  obj2: Record<
    string,
    {
      a: number;
      b: string;
    }[]
  >;
  obj3?: Record<
    string,
    {
      a: number;
      b: string;
    }
  >;
  bool: boolean;
};

const originalValue: Value = {
  str: 'default original str',
  num: 6,
  arr: [1, 2],
  arr2: [1, 2],
  arrPrefix: [{ a: 'b' }],
  arrPrefix2: [{ a2: 'str_b3', b2: 'str_c3' }],
  arrPrefix_not_default: undefined,
  zero: 0,
  undef: 'value undef',
  not_default: undefined,
  time: {
    to: TIME_RANGE_KEYS_TO.EndWeek,
    to2: 1000,
  },
  obj: {
    key: {
      a: 0,
      b: 'str',
    },
    key2: {
      a: 2,
      b: 'str2',
    },
  },
  obj2: {
    key: [
      {
        a: 0,
        b: 'str',
      },
      {
        a: 2,
        b: 'str2',
      },
    ],
  },
  obj3: {
    ob_key: {
      a: 5,
      b: '5',
    },
  },
  bool: true,
};

const defaultValue: Value = {
  str: 'default str',
  num: 1,
  arr: [],
  arr2: [1, 2],
  arrPrefix: [],
  arrPrefix2: [{ a2: 'str_b2', b2: 'str_c2' }],
  arrPrefix_not_default: undefined,
  zero: 0,
  not_default: undefined,
  time: {
    to: 0,
    to2: 0,
  },
  obj: {
    key: {
      a: 0,
      b: 'str',
    },
    key2: {
      a: 2,
      b: 'str2',
    },
  },
  obj2: {
    key: [
      {
        a: 0,
        b: 'str',
      },
      {
        a: 2,
        b: 'str2',
      },
    ],
  },
  obj3: undefined,
  bool: false,
};

const configParams: ConfigParams = {
  str: {
    default: 'default str',
    urlKey: 's',
  },
  num: {
    ...NumberParam,
    default: 1,
    urlKey: 'n',
  },
  arr: {
    ...NumberParam,
    isArray: true,
    default: [],
    urlKey: 'ar',
  },
  arr2: {
    ...NumberParam,
    isArray: true,
    default: [1, 2],
    urlKey: 'ar2',
  },
  arrPrefix: {
    isArray: true,
    prefixArray: (i) => (i ? `p${i}.` : ''),
    default: [],
    params: {
      a: {
        required: true,
      },
    },
  },
  arrPrefix2: {
    isArray: true,
    prefixArray: (i) => (i ? `p2${i}.` : ''),
    default: [],
    params: {
      a2: {
        required: true,
      },
      b2: {
        default: 'str_c2',
      },
    },
  },
  arrPrefix_not_default: {
    isArray: true,
    prefixArray: (i) => (i ? `pnd${i}.` : ''),
    params: {
      and: {
        required: true,
      },
    },
  },
  zero: {
    ...NumberParam,
    default: 0,
    urlKey: 'z',
  },
  undef: {
    default: undefined,
    urlKey: 'u',
  },
  not_default: {
    urlKey: 'nd',
  },
  time: {
    params: {
      to: {
        ...TimeToParam,
        default: 0,
      },
      to2: {
        ...TimeToParam,
        default: 0,
      },
    },
  },
  obj: {
    ...ObjectsParam('-'),
    fromEntries: true,
    default: {
      key: {
        a: 0,
        b: 'str',
      },
      key2: {
        a: 2,
        b: 'str2',
      },
    },
    urlKey: 'ob',
  },
  obj2: {
    ...ObjectsParam('-'),
    isArray: true,
    fromEntries: true,
    default: {
      key: [
        {
          a: 0,
          b: 'str',
        },
        {
          a: 2,
          b: 'str2',
        },
      ],
    },
    urlKey: 'ob2',
  },
  obj3: {
    ...ObjectsParam('|'),
    fromEntries: true,
  },
  bool: {
    ...BooleanParam,
    default: false,
  },
};

describe('QueryParamsParser', () => {
  beforeEach(() => {
    locationSearch = '';
  });

  test('decode default by param', () => {
    const v = decodeQueryParams<Value>(configParams, originalValue, new URLSearchParams(''));
    expect(v).toEqual(originalValue);
  });

  test('decode default by config', () => {
    const v = decodeQueryParams<Value>(configParams, undefined, new URLSearchParams(''));
    expect(v).toEqual({ ...defaultValue, arrPrefix2: [] });
  });

  test('encode default by config', () => {
    const p = encodeQueryParams<Value>(configParams, originalValue, undefined, new URLSearchParams(''));
    expect(p.toString()).toEqual(
      's=default+original+str&n=6&ar=1&ar=2&a=b&a2=str_b3&b2=str_c3&u=value+undef&to=ew&to2=1000&ob=key-%7B%22a%22%3A0%2C%22b%22%3A%22str%22%7D&ob=key2-%7B%22a%22%3A2%2C%22b%22%3A%22str2%22%7D&ob2=key-%7B%22a%22%3A0%2C%22b%22%3A%22str%22%7D&ob2=key-%7B%22a%22%3A2%2C%22b%22%3A%22str2%22%7D&obj3=ob_key%7C%7B%22a%22%3A5%2C%22b%22%3A%225%22%7D&bool=1'
    );
    const v2 = decodeQueryParams<Value>(configParams, undefined, new URLSearchParams(p.toString()));
    expect(v2).toEqual(originalValue);
  });

  test('decode <-> encode', () => {
    const v = decodeQueryParams<Value>(configParams, originalValue, new URLSearchParams(''));
    const p = encodeQueryParams<Value>(configParams, v!, defaultValue, new URLSearchParams(''));
    const v2 = decodeQueryParams<Value>(configParams, defaultValue, new URLSearchParams(p.toString()));
    expect(p.toString()).toEqual(
      's=default+original+str&n=6&ar=1&ar=2&a=b&a2=str_b3&b2=str_c3&u=value+undef&to=ew&to2=1000&obj3=ob_key%7C%7B%22a%22%3A5%2C%22b%22%3A%225%22%7D&bool=1'
    );
    expect(v).toEqual(v2);
  });

  test('decode <-> encode 2', () => {
    const defaultV: Value = {
      ...originalValue,
      obj: { keyT: { a: 5, b: '5' }, keyN: { a: 5, b: '5' } },
      obj2: {
        keyT: [
          { a: 5, b: '5' },
          { a: 6, b: '6' },
        ],
      },
    };
    const v = decodeQueryParams<Value>(configParams, defaultV, new URLSearchParams(''));
    const p = encodeQueryParams<Value>(configParams, v!, defaultValue, new URLSearchParams(''));
    const v2 = decodeQueryParams<Value>(configParams, defaultValue, new URLSearchParams(p.toString()));
    expect(v).toEqual(v2);
  });

  test('decode <-> encode 3', () => {
    const val: Value = {
      ...defaultValue,
      arrPrefix: [
        {
          a: '1',
        },
        {
          a: '2',
        },
        {
          a: '3',
        },
        {
          a: '4',
        },
      ],
    };
    const p = encodeQueryParams<Value>(configParams, val, defaultValue, new URLSearchParams(''));
    expect(p.toString()).toEqual('a=1&p1.a=2&p2.a=3&p3.a=4');
    const v = decodeQueryParams<Value>(configParams, defaultValue, p);
    expect(v).toEqual(val);
  });

  test('decode undefined', () => {
    const v = decodeQueryParams<Value>(configParams, defaultValue, new URLSearchParams('n=q2'));
    expect(v?.num).toBe(defaultValue.num);
  });

  test('decode ObjectsParam', () => {
    const val: Value = {
      ...defaultValue,
      obj2: {
        asd: [
          {
            a: 55,
            b: '55',
          },
          {
            a: 56,
            b: '56',
          },
        ],
      },
    };
    const p = encodeQueryParams<Value>(configParams, val, defaultValue, new URLSearchParams(''));
    expect(p.toString()).toBe(
      'ob2=asd-%7B%22a%22%3A55%2C%22b%22%3A%2255%22%7D&ob2=asd-%7B%22a%22%3A56%2C%22b%22%3A%2256%22%7D'
    );
    const v = decodeQueryParams<Value>(
      configParams,
      defaultValue,
      new URLSearchParams(
        'ob2=asd-%7B%22a%22%3A55%2C%22b%22%3A%2255%22%7D&ob2=asd%7B%22a%22%3A56%2C%22b%22%3A%2256%22%7D&obj3=asd|%7B%22a%22%3A55%2C%22b%22%3A%2255%22%7D&obj3=asd|%7B%22a%22%3A56%2C%22b%22%3A%2256%22%7D'
      )
    );
    expect(v?.obj2).toEqual({
      asd: [
        {
          a: 55,
          b: '55',
        },
      ],
    });
    expect(v?.obj3).toEqual({
      asd: {
        a: 55,
        b: '55',
      },
    });
  });

  test('default URLSearchParams', () => {
    locationSearch = 'n=500';
    const v = decodeQueryParams<Value>(configParams, defaultValue);
    expect(v?.num).toBe(500);
    const p = encodeQueryParams<Value>(configParams, v!, defaultValue);
    expect(p.toString()).toEqual('n=500');
  });

  test('parse NumberParam', () => {
    expect(NumberParam.decode?.('2')).toBe(2);
    expect(NumberParam.decode?.('q2')).toBe(undefined);
    expect(NumberParam.encode?.(2)).toBe('2');
    expect(NumberParam.encode?.(undefined)).toBe(undefined);
  });

  test('parse TimeToParam', () => {
    expect(TimeToParam.decode?.(TIME_RANGE_KEYS_TO.EndWeek)).toBe(TIME_RANGE_KEYS_TO.EndWeek);
    expect(TimeToParam.decode?.('2')).toBe(2);
    expect(TimeToParam.decode?.('w2')).toBe(undefined);
    expect(TimeToParam.encode?.(2)).toBe('2');
    expect(TimeToParam.encode?.(undefined)).toBe(undefined);
  });

  test('parse TagSyncParam', () => {
    expect(TagSyncParam.decode?.('0.3-1.3-3.2-4.0-5.0')).toEqual([3, 3, undefined, 2, 0, 0]);
    expect(TagSyncParam.encode?.([3, 3, undefined, 2, 0, 0])).toBe('0.3-1.3-3.2-4.0-5.0');
  });

  test('parse FilterParams', () => {
    expect(FilterParams().decode?.('1-messages')).toEqual(['key1', 'messages']);
    expect(FilterParams().decode?.('1~messages')).toEqual(undefined);
    expect(FilterParams().decode?.('key1-messages')).toEqual(['key1', 'messages']);
    expect(FilterParams().decode?.('key1~messages')).toEqual(undefined);
    expect(FilterParams().decode?.('key_s-messages')).toEqual(['skey', 'messages']);
    expect(FilterParams().decode?.('key_s~messages')).toEqual(undefined);
    expect(FilterParams().decode?.('_s-messages')).toEqual(['skey', 'messages']);
    expect(FilterParams().decode?.('_s~messages')).toEqual(undefined);
    expect(FilterParams().encode?.(['key1', 'messages'])).toBe('1-messages');
    expect(FilterParams().encode?.(['skey', 'messages'])).toBe('_s-messages');
    expect(FilterParams(true).decode?.('1-messages')).toEqual(undefined);
    expect(FilterParams(true).decode?.('1~messages')).toEqual(['key1', 'messages']);
    expect(FilterParams(true).decode?.('key1-messages')).toEqual(undefined);
    expect(FilterParams(true).decode?.('key1~messages')).toEqual(['key1', 'messages']);
    expect(FilterParams(true).decode?.('key_s-messages')).toEqual(undefined);
    expect(FilterParams(true).decode?.('key_s~messages')).toEqual(['skey', 'messages']);
    expect(FilterParams(true).decode?.('_s-messages')).toEqual(undefined);
    expect(FilterParams(true).decode?.('_s~messages')).toEqual(['skey', 'messages']);
    expect(FilterParams(true).encode?.(['key1', 'messages'])).toBe('1~messages');
    expect(FilterParams(true).encode?.(['skey', 'messages'])).toBe('_s~messages');
  });

  test('change FilterParams', () => {
    type Val = {
      items: {
        name: string;
        filterIn: Record<string, string[]>;
      }[];
    };
    const conf: ConfigParams = {
      items: {
        struct: true,
        isArray: true,
        prefixArray: (i) => `t${i}.`,
        params: {
          name: {
            required: true,
            urlKey: 'n',
          },
          filterIn: {
            ...FilterParams(),
            urlKey: 'f',
          },
        },
      },
    };
    const value: Val = {
      items: [
        {
          name: 'item1',
          filterIn: {
            key1: ['value1'],
            key2: ['value2'],
          },
        },
        {
          name: 'item3',
          filterIn: {
            key4: ['value4'],
            key5: ['value5'],
          },
        },
      ],
    };
    const p = encodeQueryParams<Val>(conf, value, undefined, new URLSearchParams());
    expect(p.toString()).toEqual('t0.n=item1&t0.f=1-value1&t0.f=2-value2&t1.n=item3&t1.f=4-value4&t1.f=5-value5');
    const p2 = encodeQueryParams<Val>(
      conf,
      {
        items: [
          {
            name: 'item1',
            filterIn: {
              key1: ['value1'],
              key2: ['value2'],
            },
          },
        ],
      },
      value,
      p
    );
    expect(p2.toString()).toEqual('t0.n=item1&t0.f=1-value1&t0.f=2-value2');
    const v = decodeQueryParams(conf, value, p2);
    expect(v!).toEqual({
      items: [{ filterIn: { key1: ['value1'], key2: ['value2'] }, name: 'item1' }],
    });
  });

  test('parse UseV2Param', () => {
    expect(UseV2Param.decode?.(queryValueBackendVersion2)).toBe(true);
    expect(UseV2Param.decode?.(queryValueBackendVersion1)).toBe(false);
    expect(UseV2Param.encode?.(true)).toBe(queryValueBackendVersion2);
    expect(UseV2Param.encode?.(false)).toBe(queryValueBackendVersion1);
  });

  test('parse ObjectsParam', () => {
    expect(ObjectsParam('-').decode?.('keyqwe')).toEqual(undefined);
    expect(ObjectsParam('-').decode?.('key-qwe')).toEqual(['key', 'qwe']);
    expect(ObjectsParam('-').encode?.(['key', 'qwe'])).toBe('key-qwe');
    expect(ObjectsParam('-').decode?.('key-2')).toEqual(['key', 2]);
    expect(ObjectsParam('-').encode?.(['key', 2])).toBe('key-2');
    const val = { a: 1, b: 2, z: null, s: 'str' };
    expect(ObjectsParam('-').decode?.('key-{"a":1,"b":2,"z":null,"s":"str"}')).toEqual(['key', val]);
    expect(ObjectsParam('-').encode?.(['key', val])).toBe('key-{"a":1,"b":2,"z":null,"s":"str"}');
  });

  test('parse BooleanParam', () => {
    expect(BooleanParam.decode?.('1')).toBe(true);
    expect(BooleanParam.decode?.('')).toBe(false);
    expect(BooleanParam.encode?.(true)).toBe('1');
    expect(BooleanParam.encode?.(false)).toBe('0');
    expect(BooleanParam.encode?.(undefined)).toBe('0');
  });

  test('mergeLeft', () => {
    const v = mergeLeft(defaultValue, originalValue);
    expect(v).toEqual(originalValue);
    expect(v.arr2 === defaultValue.arr2).toBeTruthy();
    expect(v.arr2 === originalValue.arr2).toBeFalsy();
  });

  test('mergeLeft 2', () => {
    type MergeLeftVal2 = {
      items: Record<string, string[]>[];
    };
    const v1: MergeLeftVal2 = {
      items: [{ key1: ['value1'] }],
    };
    const v2: MergeLeftVal2 = {
      items: [{ key4: ['value4'] }],
    };

    const v = mergeLeft<MergeLeftVal2>(v1, v2);
    expect(Object.entries(v.items[0])).toEqual([['key4', ['value4']]]);
  });
  test('mergeLeft 3', () => {
    type MergeLeftVal3 = Record<string, string>;
    const v1: MergeLeftVal3 = { key1: 'value1' };
    const v2: MergeLeftVal3 = { key4: 'value4' };
    const v = mergeLeft<MergeLeftVal3>(v1, v2);
    expect(Object.entries(v)).toEqual([['key4', 'value4']]);
  });
});
