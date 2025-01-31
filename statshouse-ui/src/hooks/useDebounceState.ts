// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useEffect, useState } from 'react';

export const useDebounceState = <T>(defaultValue: T, delay: number = 200) => {
  const [value, setValue] = useState<T>(defaultValue);
  const [debounceValue, setDebounceValue] = useState<T>(defaultValue);
  useEffect(() => {
    const timout = setTimeout(() => {
      setDebounceValue(() => value);
    }, delay);
    return () => {
      clearTimeout(timout);
    };
  }, [value, delay]);
  return [value, debounceValue, setValue] as [T, T, React.Dispatch<React.SetStateAction<T>>];
};
