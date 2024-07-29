// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { create, StateCreator, StoreMutatorIdentifier } from 'zustand';
import { devtools } from 'zustand/middleware';
import { immer } from 'zustand/middleware/immer';

export type Store<T, Mos extends [StoreMutatorIdentifier, T][] = []> = StateCreator<
  T,
  [['zustand/devtools', never], ['zustand/immer', never]],
  Mos,
  T
>;

export type StoreSlice<S, T, Mos extends [StoreMutatorIdentifier, T][] = []> = StateCreator<
  S,
  [['zustand/devtools', never], ['zustand/immer', never]],
  Mos,
  T
>;

export function createStore<T, Mos extends [StoreMutatorIdentifier, T][] = []>(
  store: Store<T, Mos>,
  name: string = ''
) {
  return create<T>()(
    devtools(immer(store), {
      name: store.name || name,
      trace: true,
      store: store.name || name,
      enabled: process.env.NODE_ENV === 'development',
    })
  );
}
