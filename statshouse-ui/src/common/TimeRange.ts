// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { TimeHelper } from './TimeHelper';

export const TIME_RANGE_KEYS_TO = {
  Now: '0',
  EndDay: 'ed',
  EndWeek: 'ew',
  default: 'd',
} as const;

export const defaultTimeRange = { to: TIME_RANGE_KEYS_TO.default, from: 0 };

export type KeysTo = (typeof TIME_RANGE_KEYS_TO)[keyof typeof TIME_RANGE_KEYS_TO];

function constToTime(now: number, value: KeysTo, ms?: boolean) {
  switch (value) {
    case TIME_RANGE_KEYS_TO.EndDay:
      return TimeHelper.getEndDay(now, ms);
    case TIME_RANGE_KEYS_TO.EndWeek:
      return TimeHelper.getEndWeek(now, ms);
    default:
      return TimeHelper.getNow(now, ms);
  }
}

export type TimeRangeData = {
  to: number | KeysTo;
  from: number;
};

export type SetTimeRangeValue = TimeRangeData | ((prev: TimeRange) => TimeRangeData);
export type SetTimeRange = (value: SetTimeRangeValue) => void;

export class TimeRange {
  protected _absolute: boolean = false;
  protected _to: number | KeysTo = 0;
  protected _from: number = 0;
  protected _ms: boolean;
  protected _nowTime: number = 0;

  constructor(range: SetTimeRangeValue, ms?: boolean) {
    this._ms = ms ?? false;
    this.setRange(range);
  }

  get to(): number {
    if (typeof this._to === 'number') {
      return this._absolute ? this._to : this._nowTime + this._to;
    }
    return constToTime(this._nowTime, this._to, this._ms);
  }

  get from(): number {
    return this.to + this._from;
  }

  get relativeTo(): number {
    if (typeof this._to === 'number') {
      return this._absolute ? this._to - this._nowTime : this._to;
    }
    return constToTime(this._nowTime, this._to, this._ms) - this._nowTime;
  }

  get relativeFrom(): number {
    return this._from;
  }

  get localNow(): number {
    return this._nowTime;
  }

  get absolute(): boolean {
    return this._absolute;
  }

  set absolute(value: boolean) {
    if (value) {
      this.setRange({
        to: this.to,
        from: this.from,
      });
    } else {
      this.setRange({
        to:
          Object.values(TIME_RANGE_KEYS_TO).find(
            (key) => Math.abs(this.to - constToTime(this._nowTime, key, this._ms)) < 60
          ) ?? TIME_RANGE_KEYS_TO.Now,
        from: this._from,
      });
    }
  }

  setRange(range: SetTimeRangeValue) {
    this._nowTime = TimeHelper.getNow(undefined, this._ms);
    const newRange = typeof range === 'function' ? range(this) : range;
    this._absolute = typeof newRange.to === 'number' && newRange.to > 0;
    this._to = newRange.to;
    this._from = newRange.from > 0 ? newRange.from - this.to : newRange.from;
    return this;
  }

  getRange() {
    return {
      to: this.to,
      from: this.from,
    };
  }

  getRangeRelative() {
    return {
      to: this.relativeTo,
      from: this.relativeFrom,
    };
  }

  getRangeUrl() {
    return {
      to: this._to,
      from: this._from,
    };
  }

  toJSON() {
    return this.getRangeUrl();
  }

  toString() {
    return JSON.stringify(this.getRangeUrl());
  }
}
