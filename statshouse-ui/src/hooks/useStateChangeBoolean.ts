// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useMemo, useState } from 'react';

export function useStateChangeBoolean(initialState: boolean | (() => boolean)) {
  const [checked, setChecked] = useState(initialState);
  const onChange = useCallback((event: React.ChangeEvent<HTMLInputElement>) => {
    const nextValue = event.currentTarget.checked;
    setChecked(nextValue);
  }, []);
  useEffect(() => {
    setChecked(initialState);
  }, [initialState]);
  return useMemo(() => ({ checked, onChange }), [onChange, checked]);
}
