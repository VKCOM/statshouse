// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

const debugEnabled = process.env.NODE_ENV !== 'production';

export const debug = {
  log(...arg: any) {
    if (debugEnabled) {
      // eslint-disable-next-line no-console
      console.log(...arg);
    }
  },
  error(...arg: any) {
    if (debugEnabled) {
      // eslint-disable-next-line no-console
      console.error(...arg);
    }
  },
  warn(...arg: any) {
    if (debugEnabled) {
      // eslint-disable-next-line no-console
      console.warn(...arg);
    }
  },
};
