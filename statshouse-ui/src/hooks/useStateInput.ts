// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useMemo, useState } from 'react';

export function useStateInput(initialState: string | (() => string)) {
  const [value, setValue] = useState(initialState);
  const onInput = useCallback((event: React.FormEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    const nextValue = event.currentTarget.value;
    setValue(nextValue);
  }, []);
  useEffect(() => {
    setValue(initialState);
  }, [initialState]);
  return useMemo(() => ({ value, onInput }), [onInput, value]);
}
