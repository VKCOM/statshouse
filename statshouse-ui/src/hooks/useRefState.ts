// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useEffect, useRef, useState } from 'react';

export function useRefState<T>(initialValue: T | null): [T | null, React.RefObject<T>] {
  const ref = useRef<T>(initialValue);
  const [value, setValue] = useState(initialValue);
  useEffect(() => {
    setValue(ref.current);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ref.current]);
  return [value, ref];
}
