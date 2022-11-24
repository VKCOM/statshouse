// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import * as api from './api';
import * as url from '../common/url';
import { URLSearchParamsInit } from 'react-router-dom';
import { useParams } from '../hooks/useParams';
import { dequal } from 'dequal/lite';
import { parseFilter } from '../common/plotQueryParams';

const queryParamAgg = 'g';
const queryParamLockMin = 'yl';
const queryParamLockMax = 'yh';

export interface lockRange {
  readonly min: number;
  readonly max: number;
}

function lockRangeFromParams(prevState: lockRange, params: URLSearchParams, default_: lockRange): lockRange {
  const newRange = {
    min: url.numberFromParams(params, '', queryParamLockMin, default_.min, true),
    max: url.numberFromParams(params, '', queryParamLockMax, default_.max, true),
  };
  if (dequal(newRange, prevState)) {
    return prevState;
  }
  return newRange;
}

function lockRangeToParams(value: lockRange, params: URLSearchParams, default_: lockRange) {
  url.numberToParams(value.min, params, '', queryParamLockMin, default_.min);
  url.numberToParams(value.max, params, '', queryParamLockMax, default_.max);
  return params;
}

export function useLockRangeURLState(
  stateUrl: URLSearchParams,
  setStateUrl: (
    nextInit: URLSearchParamsInit,
    navigateOptions?: { replace?: boolean | undefined; state?: any } | undefined
  ) => void,
  prefix: string,
  default_: lockRange,
  replace: url.replaceParams<lockRange>
) {
  return useParams(stateUrl, setStateUrl, default_, replace, lockRangeFromParams, lockRangeToParams);
}
