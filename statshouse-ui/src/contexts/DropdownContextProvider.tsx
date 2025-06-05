// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { ReactNode } from 'react';
import type { SetStateBoolean } from '@/hooks';
import { DropdownContext } from '@/contexts/DropdownContext';

export type DropdownContextProviderProps = {
  children?: ReactNode;
  value: SetStateBoolean;
};

export function DropdownContextProvider({ children, value }: DropdownContextProviderProps) {
  return <DropdownContext.Provider value={value}>{children}</DropdownContext.Provider>;
}
