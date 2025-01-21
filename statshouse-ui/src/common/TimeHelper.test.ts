// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { TimeHelper } from './TimeHelper';

const weekSec = 7 * 24 * 60 * 60;
const hours_8 = 8 * 60 * 60;

describe('TimeHelper', () => {
  test('getEndWeek', () => {
    const start = 1675025998 - weekSec;
    const end = 1675025998 + weekSec;
    for (let i = start; i <= end; i += hours_8) {
      const res = TimeHelper.getEndWeek(i);
      expect(new Date(res * 1000).getDay()).toBe(0);
      expect(res - i).toBeLessThanOrEqual(weekSec);
    }
  });
});
