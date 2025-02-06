// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import { persist } from 'zustand/middleware';
import { getNextState } from '@/common/getNextState';
import { localStorageDefault } from '@/common/localStorageDefault';
import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';

export type DevStore = {
  enabled: boolean;
};

function getDefault() {
  return {
    enabled: false,
  };
}

function DevStoreEqual(store: DevStore): boolean {
  return store.enabled === getDefault().enabled;
}

export const useStoreDev = create<DevStore>()(
  persist(
    immer(() => ({
      ...getDefault(),
    })),
    {
      name: 'sh-dev',
      storage: localStorageDefault(DevStoreEqual),
    }
  )
);

export function setDevEnabled(nextState: React.SetStateAction<boolean>) {
  useStoreDev.setState((state) => {
    state.enabled = getNextState(state.enabled, nextState);
  });
}
