// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { TIME_RANGE_ABBREV, type TimeRangeAbbrev } from '@/api/enum';
import type { TimeRange } from '@/url2';

export function timeRangeAbbrevExpand(abbr: TimeRangeAbbrev): number {
  switch (abbr) {
    case TIME_RANGE_ABBREV.last5m:
      return -60 * 5;
    case TIME_RANGE_ABBREV.last15m:
      return -60 * 15;
    case TIME_RANGE_ABBREV.last1h:
      return -3600;
    case TIME_RANGE_ABBREV.last2h:
      return -3600 * 2;
    case TIME_RANGE_ABBREV.last6h:
      return -3600 * 6;
    case TIME_RANGE_ABBREV.last12h:
      return -3600 * 12;
    case TIME_RANGE_ABBREV.last1d:
      return -3600 * 24;
    case TIME_RANGE_ABBREV.last2d:
      return -3600 * 24 * 2;
    case TIME_RANGE_ABBREV.last3d:
      return -3600 * 24 * 3;
    case TIME_RANGE_ABBREV.last7d:
      return -3600 * 24 * 7;
    case TIME_RANGE_ABBREV.last14d:
      return -3600 * 24 * 14;
    case TIME_RANGE_ABBREV.last30d:
      return -3600 * 24 * 30;
    case TIME_RANGE_ABBREV.last90d:
      return -3600 * 24 * 90;
    case TIME_RANGE_ABBREV.last180d:
      return -3600 * 24 * 180;
    case TIME_RANGE_ABBREV.last1y:
      return -3600 * 24 * 365;
    case TIME_RANGE_ABBREV.last2y:
      return -3600 * 24 * 365 * 2;
    default:
      return -3600 * 2;
  }
}

export function getAbbrev(timeRange: TimeRange): TimeRangeAbbrev | '' {
  const tolerance = 60;
  return (
    Object.values(TIME_RANGE_ABBREV).find((abbrKey) => {
      const rr = timeRangeAbbrevExpand(abbrKey);
      return Math.abs(rr - timeRange.from) <= tolerance && Math.abs(timeRange.to - timeRange.now) <= tolerance;
    }) ?? ''
  );
}
