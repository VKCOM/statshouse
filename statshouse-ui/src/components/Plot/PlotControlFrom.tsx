// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { ChangeEvent, memo, useCallback, useMemo } from 'react';
import { SetTimeRange, TIME_RANGE_KEYS_TO, TimeRange } from '../../common/TimeRange';
import { TimeHelper } from '../../common/TimeHelper';
import { now, timeRangeAbbrev, timeRangeAbbrevExpand, timeRangeString, timeRangeToAbbrev } from '../../view/utils';
import cn from 'classnames';

export type PlotControlFromProps = {
  timeRange: TimeRange;
  setTimeRange: SetTimeRange;
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
      setTimeRange((range) => timeRangeAbbrevExpand(abbr, range.absolute ? now() : 0));
    },
    [setBaseRange, setTimeRange]
  );

  const onTodayClick = useCallback(() => {
    setBaseRange('last-1d');
    const endDay = TimeHelper.getEndDay();
    setTimeRange((range) => ({
      to: range.absolute ? endDay : TIME_RANGE_KEYS_TO.EndDay,
      from: timeRangeAbbrevExpand('last-1d', endDay).from,
    }));
  }, [setBaseRange, setTimeRange]);

  const disableTodayClick = useMemo(
    () => timeRange.relativeFrom === -86400 && timeRange.to === TimeHelper.getEndDay(),
    [timeRange]
  );

  const onWeekClick = useCallback(() => {
    setBaseRange('last-1d');
    const endWeek = TimeHelper.getEndWeek();
    setTimeRange((range) => ({
      to: range.absolute ? endWeek : TIME_RANGE_KEYS_TO.EndWeek,
      from: timeRangeAbbrevExpand('last-7d', endWeek).from,
    }));
  }, [setBaseRange, setTimeRange]);

  const disableWeekClick = useMemo(
    () => timeRange.relativeFrom === -604800 && timeRange.to === TimeHelper.getEndWeek(),
    [timeRange]
  );

  return (
    <div className={cn('input-group flex-nowrap', className)}>
      <select
        className={`form-select ${classNameSelect}`}
        value={timeRangeToAbbrev(timeRange)}
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

      <button className="btn btn-outline-primary" type="button" onClick={onTodayClick} disabled={disableTodayClick}>
        Today
      </button>
      <button className="btn btn-outline-primary" type="button" onClick={onWeekClick} disabled={disableWeekClick}>
        Week
      </button>
    </div>
  );
};

export const PlotControlFrom = memo(_PlotControlFrom);
