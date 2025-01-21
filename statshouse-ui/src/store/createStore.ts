// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { create, StateCreator, StoreMutatorIdentifier } from 'zustand';
import { createWithEqualityFn } from 'zustand/traditional';
import { devtools } from 'zustand/middleware';
import { immer } from 'zustand/middleware/immer';

const enabled = false;

export function createStore<T, Mos extends [StoreMutatorIdentifier, T][] = []>(
  store: StateCreator<T, [['zustand/immer', never]], Mos, T>,
  name: string = ''
) {
  const storeImmer = immer(store);
  if (process.env.NODE_ENV === 'development') {
    return create<T>()(
      devtools(storeImmer, {
        name: store.name || name,
        trace: true,
        store: store.name || name,
        anonymousActionType: 'setState',
        enabled,
      })
    );
  }
  return create<T>()(storeImmer);
}

export function createStoreWithEqualityFn<T, Mos extends [StoreMutatorIdentifier, T][] = []>(
  store: StateCreator<T, [['zustand/immer', never]], Mos, T>,
  name: string = ''
) {
  if (process.env.NODE_ENV === 'development') {
    return createWithEqualityFn<T>()(
      devtools(immer(store), {
        name: store.name || name,
        trace: true,
        store: store.name || name,
        anonymousActionType: 'setState',
        enabled,
      }),
      Object.is
    );
  }
  return createWithEqualityFn<T>()(immer(store), Object.is);
}
