// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { ChangeEvent, memo, useCallback, useMemo } from 'react';

import cn from 'classnames';
import { getEndDay, getEndWeek, type TimeRange } from 'store2';
import { timeRangeAbbrev, timeRangeAbbrevExpand, timeRangeString, timeRangeToAbbrev2 } from '../../../view/utils';
import { Button } from 'components';
import { produce } from 'immer';
import { TIME_RANGE_KEYS_TO } from '../../../api/enum';

export type PlotControlFromProps = {
  timeRange: TimeRange;
  setTimeRange: (timeRange: TimeRange) => void;
  setBaseRange: (r: timeRangeAbbrev) => void;
  className?: string;
  classNameSelect?: string;
};

export const _PlotControlFrom: React.FC<PlotControlFromProps> = ({
  timeRange,
  setTimeRange,
  setBaseRange,
  className,
  classNameSelect,
}) => {
  const onTimeRangeChange = useCallback(
    (e: ChangeEvent<HTMLSelectElement>) => {
      const abbr = e.target.value as timeRangeAbbrev;
      setBaseRange(abbr);
      // setTimeRange((range) => timeRangeAbbrevExpand(abbr, range.absolute ? getNow() : 0));
    },
    [setBaseRange]
  );

  const onTodayClick = useCallback(() => {
    setBaseRange('last-1d');
    const endDay = getEndDay();
    setTimeRange(
      produce(timeRange, (t) => {
        t.to = endDay;
        if (t.absolute) {
          t.urlTo = endDay;
        } else {
          t.urlTo = TIME_RANGE_KEYS_TO.EndDay;
        }
        t.from = timeRangeAbbrevExpand('last-1d', endDay).from;
      })
    );
    // setTimeRange((range) => ({
    //   to: range.absolute ? endDay : TIME_RANGE_KEYS_TO.EndDay,
    //   from: timeRangeAbbrevExpand('last-1d', endDay).from,
    // }));
  }, [setBaseRange, setTimeRange, timeRange]);

  const disableTodayClick = useMemo(() => timeRange.from === -86400 && timeRange.to === getEndDay(), [timeRange]);

  const onWeekClick = useCallback(() => {
    setBaseRange('last-1d');
    const endWeek = getEndWeek();
    // setTimeRange((range) => ({
    //   to: range.absolute ? endWeek : TIME_RANGE_KEYS_TO.EndWeek,
    //   from: timeRangeAbbrevExpand('last-7d', endWeek).from,
    // }));
    setTimeRange(
      produce(timeRange, (t) => {
        t.to = endWeek;
        if (t.absolute) {
          t.urlTo = endWeek;
        } else {
          t.urlTo = TIME_RANGE_KEYS_TO.EndWeek;
        }
        t.from = timeRangeAbbrevExpand('last-1d', endWeek).from;
      })
    );
  }, [setBaseRange, setTimeRange, timeRange]);

  const disableWeekClick = useMemo(() => timeRange.from === -604800 && timeRange.to === getEndWeek(), [timeRange]);

  return (
    <div className={cn('input-group flex-nowrap', className)}>
      <select
        className={`form-select ${classNameSelect}`}
        value={timeRangeToAbbrev2(timeRange)}
        onChange={onTimeRangeChange}
      >
        <option value="" disabled>
          {timeRangeString(timeRange)}
        </option>
        <option value="last-5m">Last 5 minutes</option>
        <option value="last-15m">Last 15 minutes</option>
        <option value="last-1h">Last hour</option>
        <option value="last-2h">Last 2 hours</option>
        <option value="last-6h">Last 6 hours</option>
        <option value="last-12h">Last 12 hours</option>
        <option value="last-1d">Last 24 hours</option>
        <option value="last-2d">Last 48 hours</option>
        <option value="last-3d">Last 72 hours</option>
        <option value="last-7d">Last 7 days</option>
        <option value="last-14d">Last 14 days</option>
        <option value="last-30d">Last 30 days</option>
        <option value="last-90d">Last 90 days</option>
        <option value="last-180d">Last 180 days</option>
        <option value="last-1y">Last year</option>
        <option value="last-2y">Last 2 years</option>
      </select>

      <Button className="btn btn-outline-primary" type="button" onClick={onTodayClick} disabled={disableTodayClick}>
        Today
      </Button>
      <Button className="btn btn-outline-primary" type="button" onClick={onWeekClick} disabled={disableWeekClick}>
        Week
      </Button>
    </div>
  );
};

export const PlotControlFrom = memo(_PlotControlFrom);
