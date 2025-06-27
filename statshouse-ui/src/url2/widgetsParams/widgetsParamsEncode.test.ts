// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import '@/testMock/matchMedia.mock';
import { getDefaultParams, getNewMetric, QueryParams, removeValueChar, widgetsParamsEncode } from '@/url2';

const params: QueryParams = getDefaultParams();

describe('@/urlStore widgetsParamsEncode', () => {
  test('@/urlEncodePlots', () => {
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
      // ['op', '1'],
    ]);
  });
});
