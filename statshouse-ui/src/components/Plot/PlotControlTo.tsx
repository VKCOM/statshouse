// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { ChangeEvent, memo, useCallback } from 'react';
import { SetTimeRange, TimeRange } from '../../common/TimeRange';
import { formatInputDate, formatInputTime, maxTimeRange, now, parseInputDate, parseInputTime } from '../../view/utils';
import { ReactComponent as SVGLockClock } from '../../assets/svg/LockClock.svg';
import { ReactComponent as SVGUnlockClock } from '../../assets/svg/UnlockClock.svg';
import cn from 'classnames';

export type PlotControlToProps = {
  timeRange: TimeRange;
  setTimeRange: SetTimeRange;
  className?: string;
  classNameInput?: string;
};

export const _PlotControlTo: React.FC<PlotControlToProps> = ({
  timeRange,
  setTimeRange,
  className,
  classNameInput,
}) => {
  const onRelativeToChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      const absolute = !e.target.checked;
      setTimeRange((range) => {
        range.absolute = absolute;
        return range.getRangeUrl();
      });
    },
    [setTimeRange]
  );

  const onToDateChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      const v = e.target.value;
      const [y] = parseInputDate(v);
      if (v === '' || y < 1900) {
        return;
      }

      setTimeRange((range) => {
        const [y, m, d] = parseInputDate(v);
        const t = new Date(range.to * 1000);
        return {
          to: Math.floor(t.setFullYear(y, m, d) / 1000),
          from: range.relativeFrom,
        };
      });
    },
    [setTimeRange]
  );

  const onToTimeChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      const v = e.target.value;

      setTimeRange((range) => {
        const [h, m, sec] = v !== '' ? parseInputTime(v) : [0, 0, 0];
        const t = new Date(range.to * 1000);
        return {
          to: Math.floor(t.setHours(h, m, sec) / 1000),
          from: range.relativeFrom,
        };
      });
    },
    [setTimeRange]
  );

  return (
    <div className={cn('input-group flex-nowrap', className)}>
      <input
        type="date"
        className={`form-control form-control-safari-fix ${classNameInput}`}
        disabled={!timeRange.absolute}
        max={formatInputDate(Math.min(timeRange.from + maxTimeRange, now()))}
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
      <input
        type="checkbox"
        className="btn-check"
        id="toggle-relative"
        autoComplete="off"
        checked={!timeRange.absolute}
        onChange={onRelativeToChange}
      />
      <label className="btn btn-outline-primary" htmlFor="toggle-relative" title="Use relative date/time in URL">
        {!timeRange.absolute ? <SVGLockClock /> : <SVGUnlockClock />}
      </label>
    </div>
  );
};

export const PlotControlTo = memo(_PlotControlTo);
