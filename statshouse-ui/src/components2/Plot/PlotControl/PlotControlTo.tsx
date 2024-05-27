// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { ChangeEvent, memo, useCallback } from 'react';

import { formatInputDate, formatInputTime, maxTimeRange, now, parseInputDate, parseInputTime } from 'view/utils';
import { ReactComponent as SVGLockClock } from 'assets/svg/LockClock.svg';
import { ReactComponent as SVGUnlockClock } from 'assets/svg/UnlockClock.svg';
import cn from 'classnames';
import { TimeRange } from 'store2';
import { ToggleButton } from 'components';
import { produce } from 'immer';

export type PlotControlToProps = {
  timeRange: TimeRange;
  setTimeRange: (timeRange: TimeRange) => void;
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
    (status: boolean) => {
      // setTimeRange((range) => {
      //   range.absolute = !status;
      //   return range.getRangeUrl();
      // });
      setTimeRange(
        produce(timeRange, (t) => {
          t.absolute = !status;
        })
      );
    },
    [setTimeRange, timeRange]
  );

  const onToDateChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      const v = e.target.value;
      const [y, m, d] = parseInputDate(v);
      if (v === '' || y < 1900) {
        return;
      }
      const t = new Date(timeRange.to * 1000);
      t.setFullYear(y, m, d);
      setTimeRange(
        produce(timeRange, (t) => {
          t.urlTo = t.to = Math.floor(+t / 1000);
        })
      );
      // setTimeRange((range) => {
      //   const [y, m, d] = parseInputDate(v);
      //   const t = new Date(range.to * 1000);
      //   return {
      //     to: Math.floor(t.setFullYear(y, m, d) / 1000),
      //     from: range.relativeFrom,
      //   };
      // });
    },
    [setTimeRange, timeRange]
  );

  const onToTimeChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      const v = e.target.value;
      const [h, m, sec] = v !== '' ? parseInputTime(v) : [0, 0, 0];
      const t = new Date(timeRange.to * 1000);
      t.setHours(h, m, sec);
      setTimeRange(
        produce(timeRange, (t) => {
          t.urlTo = t.to = Math.floor(+t / 1000);
        })
      );
      // setTimeRange((range) => {
      //   const [h, m, sec] = v !== '' ? parseInputTime(v) : [0, 0, 0];
      //   const t = new Date(range.to * 1000);
      //   return {
      //     to: Math.floor(t.setHours(h, m, sec) / 1000),
      //     from: range.relativeFrom,
      //   };
      // });
    },
    [setTimeRange, timeRange]
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
};

export const PlotControlTo = memo(_PlotControlTo);
