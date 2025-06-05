// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { ReactNode } from 'react';
import { OuterInfoContext } from '@/contexts/OuterInfoContext';

export type OuterInfoContextProviderProps = {
  children?: ReactNode;
  value: string;
};

export function OuterInfoContextProvider({ children, value }: OuterInfoContextProviderProps) {
  return <OuterInfoContext.Provider value={value}>{children}</OuterInfoContext.Provider>;
}
