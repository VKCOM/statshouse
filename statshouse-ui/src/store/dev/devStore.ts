// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { immer } from 'zustand/middleware/immer';
import { getNextState } from '../../common/getNextState';
import { localStorageDefault } from '../../common/localStorageDefault';

export type DevStore = {
  enabled: boolean;
  setEnabled(nextState: React.SetStateAction<boolean>): void;
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
  immer(
    persist(
      (setState) => ({
        ...getDefault(),
        setEnabled(nextState) {
          setState((state) => {
            state.enabled = getNextState(state.enabled, nextState);
          });
        },
      }),
      {
        name: 'sh-dev',
        storage: localStorageDefault(DevStoreEqual),
      }
    )
  )
);
