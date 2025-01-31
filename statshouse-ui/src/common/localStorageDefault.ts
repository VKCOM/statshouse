// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { PersistStorage, StorageValue } from 'zustand/middleware';

export function localStorageDefault<T>(defaultEqual: (state: T) => boolean): PersistStorage<T> {
  return {
    getItem(name) {
      try {
        return JSON.parse(localStorage.getItem(name) ?? 'null') as StorageValue<T> | null;
      } catch (_) {
        return null;
      }
    },
    setItem(name, value) {
      if (defaultEqual(value.state)) {
        localStorage.removeItem(name);
      } else {
        localStorage.setItem(name, JSON.stringify(value));
      }
    },
    removeItem(name: string) {
      localStorage.removeItem(name);
    },
  };
}
