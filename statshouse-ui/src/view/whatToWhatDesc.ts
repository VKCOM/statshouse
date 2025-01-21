// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// XXX: keep in sync with Go
import { QUERY_WHAT, QueryWhat } from '../api/enum';

export function whatToWhatDesc(what: QueryWhat | string): string {
  switch (what) {
    case QUERY_WHAT.p0_1:
      return 'p0.1';
    case QUERY_WHAT.p999:
      return 'p99.9';
    case QUERY_WHAT.countNorm:
    case QUERY_WHAT.countSec:
      return 'count/sec';
    case QUERY_WHAT.cuCount:
      return 'count (cumul)';
    case QUERY_WHAT.cardinalityNorm:
    case QUERY_WHAT.cardinalitySec:
      return 'cardinality/sec';
    case QUERY_WHAT.cuCardinality:
      return 'cardinality (cumul)';
    case QUERY_WHAT.cuAvg:
      return 'avg (cumul)';
    case QUERY_WHAT.sumNorm:
    case QUERY_WHAT.sumSec:
      return 'sum/sec';
    case QUERY_WHAT.cuSum:
      return 'sum (cumul)';
    case QUERY_WHAT.uniqueNorm:
    case QUERY_WHAT.uniqueSec:
      return 'unique/sec';
    case QUERY_WHAT.maxCountHost:
      return 'max(count)@host';
    case QUERY_WHAT.maxHost:
      return 'max(value)@host';
    case QUERY_WHAT.dvCount:
      return 'count (derivative)';
    case QUERY_WHAT.dvSum:
      return 'sum (derivative)';
    case QUERY_WHAT.dvAvg:
      return 'avg (derivative)';
    case QUERY_WHAT.dvCountNorm:
      return 'count/sec (derivative)';
    case QUERY_WHAT.dvSumNorm:
      return 'sum/sec (derivative)';
    case QUERY_WHAT.dvMin:
      return 'min (derivative)';
    case QUERY_WHAT.dvMax:
      return 'max (derivative)';
    case QUERY_WHAT.dvUnique:
      return 'unique (derivative)';
    case QUERY_WHAT.dvUniqueNorm:
      return 'unique/sec (derivative)';
    default:
      return what as string;
  }
}
