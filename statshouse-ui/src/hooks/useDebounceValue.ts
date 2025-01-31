// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useEffect, useState } from 'react';

export function useDebounceValue<T>(value: T, delay: number = 200) {
  const [debounceValue, setDebounceValue] = useState<T>(value);

  useEffect(() => {
    const timout = setTimeout(() => {
      setDebounceValue(() => value);
    }, delay);
    return () => {
      clearTimeout(timout);
    };
  }, [delay, value]);

  return debounceValue;
}
