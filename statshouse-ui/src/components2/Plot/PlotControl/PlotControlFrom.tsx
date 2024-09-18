// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { ChangeEvent, memo, useCallback, useMemo } from 'react';

import cn from 'classnames';
import { Button } from 'components/UI';
import { TIME_RANGE_ABBREV, TIME_RANGE_ABBREV_DESCRIPTION, TIME_RANGE_KEYS_TO, toTimeRangeAbbrev } from 'api/enum';
import { defaultBaseRange, useStatsHouseShallow } from 'store2';
import { getEndDay, getEndWeek, getNow } from 'url2';
import { getAbbrev, timeRangeAbbrevExpand } from 'store2/helpers';
import { secondsRangeToString } from 'view/utils2';

export type PlotControlFromProps = {
  className?: string;
  classNameSelect?: string;
};

export const _PlotControlFrom: React.FC<PlotControlFromProps> = ({ className, classNameSelect }) => {
  const { timeRange, setBaseRange, setTimeRange } = useStatsHouseShallow(
    ({ params: { timeRange }, setBaseRange, setTimeRange }) => ({ timeRange, setBaseRange, setTimeRange })
  );

  const onTimeRangeChange = useCallback(
    (e: ChangeEvent<HTMLSelectElement>) => {
      const abbr = toTimeRangeAbbrev(e.target.value, defaultBaseRange);
      setBaseRange(abbr);
      setTimeRange({ from: timeRangeAbbrevExpand(abbr), to: timeRange.absolute ? getNow() : TIME_RANGE_KEYS_TO.Now });
    },
    [setBaseRange, setTimeRange, timeRange.absolute]
  );

  const onTodayClick = useCallback(() => {
    setBaseRange(TIME_RANGE_ABBREV.last1d);
    setTimeRange({
      from: timeRangeAbbrevExpand(TIME_RANGE_ABBREV.last1d),
      to: timeRange.absolute ? getEndDay() : TIME_RANGE_KEYS_TO.EndDay,
    });
  }, [setBaseRange, setTimeRange, timeRange.absolute]);

  const disableTodayClick = useMemo(
    () => timeRange.from === -86400 && Math.abs(timeRange.to - getEndDay()) < 60,
    [timeRange.from, timeRange.to]
  );

  const onWeekClick = useCallback(() => {
    setBaseRange(TIME_RANGE_ABBREV.last7d);
    setTimeRange({
      from: timeRangeAbbrevExpand(TIME_RANGE_ABBREV.last7d),
      to: timeRange.absolute ? getEndWeek() : TIME_RANGE_KEYS_TO.EndWeek,
    });
  }, [setBaseRange, setTimeRange, timeRange.absolute]);

  const disableWeekClick = useMemo(
    () => timeRange.from === -604800 && Math.abs(timeRange.to - getEndWeek()) < 60,
    [timeRange.from, timeRange.to]
  );

  return (
    <div className={cn('input-group flex-nowrap', className)}>
      <select className={`form-select ${classNameSelect}`} value={getAbbrev(timeRange)} onChange={onTimeRangeChange}>
        <option value="" disabled>
          {secondsRangeToString(-timeRange.from)}
        </option>
        {Object.entries(TIME_RANGE_ABBREV_DESCRIPTION).map(([traKey, traDesc]) => (
          <option key={traKey} value={traKey}>
            {traDesc}
          </option>
        ))}
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
