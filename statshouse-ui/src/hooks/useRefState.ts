// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { RefCallback, useCallback, useState } from 'react';

export function useRefState<T = undefined>(initialValue?: T): [T | null, RefCallback<T>] {
  const [value, setValue] = useState<T | null>(initialValue ?? null);
  return [value, useCallback<RefCallback<T>>(setValue, [setValue])];
}
