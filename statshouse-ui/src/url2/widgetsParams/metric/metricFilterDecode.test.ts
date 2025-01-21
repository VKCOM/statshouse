// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import '@/testMock/matchMedia.mock';
import { getNewMetric } from './getNewMetric';
import { metricFilterDecode } from './metricFilterDecode';
import { GET_PARAMS } from '../../../api/enum';
import { toTreeObj } from '../../urlHelpers';
import { filterInSep, filterNotInSep } from '../../constants';

describe('@/urlStore widgetsParams/metric/metricFilterDecode.ts', () => {
  test('metricFilterDecode', () => {
    const dParams = {
      ...getNewMetric(),
      filterIn: {},
      filterNotIn: {},
    };
    expect(metricFilterDecode(GET_PARAMS.metricFilter, {}, dParams)).toEqual({ filterIn: {}, filterNotIn: {} });
    expect(metricFilterDecode(GET_PARAMS.metricFilter, {})).toEqual({ filterIn: {}, filterNotIn: {} });
    expect(metricFilterDecode(GET_PARAMS.metricFilter)).toEqual({ filterIn: {}, filterNotIn: {} });
    expect(metricFilterDecode(GET_PARAMS.metricFilter, { [GET_PARAMS.metricFilter]: {} }, dParams)).toEqual({
      filterIn: {},
      filterNotIn: {},
    });
    expect(
      metricFilterDecode(
        GET_PARAMS.metricFilter,
        toTreeObj({
          [GET_PARAMS.metricFilter]: ['0' + filterInSep + 'tag-value', '1' + filterNotInSep + 'tag-value'],
        }),
        dParams
      )
    ).toEqual({ filterIn: { '0': ['tag-value'] }, filterNotIn: { '1': ['tag-value'] } });
    expect(
      metricFilterDecode(
        GET_PARAMS.metricFilter,
        toTreeObj({
          [GET_PARAMS.metricFilter]: [
            '1' + filterNotInSep + 'tag4' + filterInSep + 'value',
            '1' + filterNotInSep + 'tag2' + filterInSep + 'value',
            '0' + filterInSep + 'tag4' + filterNotInSep + 'value',
            '0' + filterInSep + 'tag3' + filterNotInSep + 'value',
          ],
        }),
        dParams
      )
    ).toEqual({
      filterIn: { '0': ['tag3' + filterNotInSep + 'value', 'tag4' + filterNotInSep + 'value'] },
      filterNotIn: { '1': ['tag2' + filterInSep + 'value', 'tag4' + filterInSep + 'value'] },
    });
  });
});
