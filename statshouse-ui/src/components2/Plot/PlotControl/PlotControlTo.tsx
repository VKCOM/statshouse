// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { type ChangeEvent, memo, useCallback } from 'react';

import { ReactComponent as SVGLockClock } from '@/assets/svg/LockClock.svg';
import { ReactComponent as SVGUnlockClock } from '@/assets/svg/UnlockClock.svg';
import cn from 'classnames';
import { ToggleButton } from '@/components/UI';
import { TIME_RANGE_KEYS_TO } from '@/api/enum';
import { constToTime, readTimeRange } from '@/url2';
import { formatInputDate, formatInputTime, maxTimeRange, now, parseInputDate, parseInputTime } from '@/view/utils2';
import { StatsHouseStore, useStatsHouse } from '@/store2';
import { setParams } from '@/store2/methods';

export type PlotControlToProps = {
  className?: string;
  classNameInput?: string;
};

const selectorStore = ({ params: { timeRange } }: StatsHouseStore) => timeRange;

export const PlotControlTo = memo(function PlotControlTo({ className, classNameInput }: PlotControlToProps) {
  const timeRange = useStatsHouse(selectorStore);

  const onRelativeToChange = useCallback(
    (status: boolean) => {
      if (status) {
        setParams((p) => {
          p.timeRange = readTimeRange(
            timeRange.from,
            Object.values(TIME_RANGE_KEYS_TO).find(
              (key) => Math.abs(timeRange.to - constToTime(timeRange.now, key)) < 60
            ) ?? TIME_RANGE_KEYS_TO.Now
          );
        });
      } else {
        setParams((p) => {
          p.timeRange = readTimeRange(timeRange.from, timeRange.to);
        });
      }
    },
    [timeRange.from, timeRange.now, timeRange.to]
  );

  const onToDateChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      const v = e.target.value;
      const [y, m, d] = parseInputDate(v);
      if (v === '' || y < 1900) {
        return;
      }
      const nextTime = new Date(timeRange.to * 1000);
      nextTime.setFullYear(y, m, d);
      setParams((p) => {
        p.timeRange = readTimeRange(timeRange.from, Math.floor(+nextTime / 1000));
      });
    },
    [timeRange.from, timeRange.to]
  );

  const onToTimeChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      const v = e.target.value;
      const [h, m, sec] = v !== '' ? parseInputTime(v) : [0, 0, 0];
      const nextTime = new Date(timeRange.to * 1000);
      nextTime.setHours(h, m, sec);
      setParams((p) => {
        p.timeRange = readTimeRange(timeRange.from, Math.floor(+nextTime / 1000));
      });
    },
    [timeRange.from, timeRange.to]
  );

  return (
    <div className={cn('input-group flex-nowrap', className)}>
      <input
        type="date"
        className={`form-control form-control-safari-fix ${classNameInput}`}
        disabled={!timeRange.absolute}
        max={formatInputDate(Math.min(timeRange.to + timeRange.from + maxTimeRange, now()))}
        value={formatInputDate(timeRange.to)}
        onChange={onToDateChange}
      />
      <input
        type="time"
        className={`form-control form-control-safari-fix ${classNameInput}`}
        disabled={!timeRange.absolute}
        step="1"
        value={formatInputTime(timeRange.to)}
        onChange={onToTimeChange}
      />
      <ToggleButton
        className="btn btn-outline-primary"
        title="Use relative date/time in URL"
        checked={!timeRange.absolute}
        onChange={onRelativeToChange}
      >
        {!timeRange.absolute ? <SVGLockClock /> : <SVGUnlockClock />}
      </ToggleButton>
    </div>
  );
});
