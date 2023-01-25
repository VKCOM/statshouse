// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useCallback, useEffect, useRef, useState } from 'react';
import type { TimeRangeData } from '../common/TimeRange';
import { TIME_RANGE_KEYS_TO, KeysTo, TimeRange, SetTimeRangeValue } from '../common/TimeRange';
import * as url from '../common/url';
import { replaceParams, stringToParams } from '../common/url';
import * as api from '../view/api';
import { URLSearchParamsInit } from 'react-router-dom';

export function timeRangeToFromParams(
  params: URLSearchParams,
  prefix: string,
  name: string,
  default_: number | KeysTo
): number | KeysTo {
  name = url.join(prefix, name);
  const v = params.get(name);

  if (v) {
    if (Object.values(TIME_RANGE_KEYS_TO).includes(v as KeysTo)) {
      return v as KeysTo;
    }
    return parseInt(v);
  }
  return default_;
}

export function timeRangeToToParams(
  value: number | KeysTo,
  params: URLSearchParams,
  prefix: string,
  name: string,
  default_: number | KeysTo
) {
  stringToParams(value.toString(), params, prefix, name, default_.toString());
}

function timeRangeFromParams(params: URLSearchParams, prefix: string, default_: TimeRangeData): TimeRangeData {
  return {
    from: url.numberFromParams(params, prefix, api.queryParamFromTime, default_.from, false),
    to: timeRangeToFromParams(params, prefix, api.queryParamToTime, default_.to),
  };
}

function timeRangeToParams(value: TimeRangeData, params: URLSearchParams, prefix: string, default_: TimeRangeData) {
  url.numberToParams(value.from, params, prefix, api.queryParamFromTime, default_.from);
  timeRangeToToParams(value.to, params, prefix, api.queryParamToTime, default_.to);
}

function sortedParams(params: URLSearchParams): string {
  const p = new URLSearchParams(params);
  p.sort();
  return p.toString();
}

export const useTimeRange = (
  stateUrl: URLSearchParams,
  setStateUrl: (
    nextInit: URLSearchParamsInit,
    navigateOptions?: { replace?: boolean | undefined; state?: any } | undefined
  ) => void,
  defaultRange: TimeRangeData,
  replace: replaceParams<TimeRange>
): [TimeRange, (value: SetTimeRangeValue, force?: boolean) => void] => {
  const timeRangeRef = useRef<TimeRange>(new TimeRange(timeRangeFromParams(stateUrl, '', defaultRange)));
  const [timeRange, setTimeRange] = useState<TimeRange>(timeRangeRef.current);
  useEffect(() => {
    setTimeRange((r) => {
      const newRange = timeRangeFromParams(stateUrl, '', defaultRange);
      const { to, from } = r.getRangeUrl();
      if (newRange.to !== to || newRange.from !== from) {
        return (timeRangeRef.current = new TimeRange(newRange));
      }
      return r;
    });
  }, [defaultRange, stateUrl]);

  const updateTimeRange = useCallback(
    (value: SetTimeRangeValue, force?: boolean) => {
      const newTimeRange = new TimeRange(timeRangeRef.current.getRangeUrl());
      newTimeRange.setRange(value);
      const replaceUrl = replace(timeRangeRef.current, newTimeRange);
      const params = new URLSearchParams(document.location.search);
      timeRangeToParams(newTimeRange.getRangeUrl(), params, '', defaultRange);
      if (sortedParams(params) !== sortedParams(new URLSearchParams(document.location.search))) {
        setStateUrl(params, { replace: replaceUrl });
      }
      if (force) {
        timeRangeRef.current = new TimeRange(timeRangeRef.current.getRangeUrl());
        setTimeRange(timeRangeRef.current);
      }
    },
    [defaultRange, replace, setStateUrl]
  );
  return [timeRange, updateTimeRange];
};
