// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

export class TimeHelper {
  static toTimeStamp(time: number, ms?: boolean): number {
    if (ms) {
      return Math.floor(time / 1000) * 1000;
    }
    return Math.floor(time / 1000);
  }

  static getNow(now?: number, ms?: boolean): number {
    if (typeof now === 'number') {
      return now;
    }
    return TimeHelper.toTimeStamp(Date.now(), ms);
  }

  static getEndDay(now?: number, ms?: boolean) {
    let time;
    if (typeof now === 'number') {
      time = new Date(now * (ms ? 1 : 1000));
    } else {
      time = new Date();
    }
    time.setHours(23, 59, 59, 0);
    return TimeHelper.toTimeStamp(+time, ms);
  }

  static getEndWeek(now?: number, ms?: boolean) {
    let time;
    if (typeof now === 'number') {
      time = new Date(now * (ms ? 1 : 1000));
    } else {
      time = new Date();
    }
    time.setHours(23, 59, 59, 0);
    time.setDate(time.getDate() - (time.getDay() || 7) + 7);
    return TimeHelper.toTimeStamp(+time, ms);
  }
}
