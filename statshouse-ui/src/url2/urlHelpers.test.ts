// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import '@/testMock/matchMedia.mock';
import { arrToObj } from './urlHelpers';

describe('@/urlHelpers', () => {
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
