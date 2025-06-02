// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useMemo, useState } from 'react';

export type SetStateBoolean = { on: () => void; off: () => void; toggle: () => void };

export function useStateBoolean(init: boolean): [boolean, SetStateBoolean] {
  const [status, setStatus] = useState<boolean>(init);
  const control = useMemo(
    () => ({ on: () => setStatus(true), off: () => setStatus(false), toggle: () => setStatus((s) => !s) }),
    []
  );
  return [status, control];
}
