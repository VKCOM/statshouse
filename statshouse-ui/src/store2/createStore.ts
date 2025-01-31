// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { create, StateCreator, StoreMutatorIdentifier } from 'zustand';
import { immer } from 'zustand/middleware/immer';

export type Store<T, Mos extends [StoreMutatorIdentifier, T][] = []> = StateCreator<
  T,
  [['zustand/immer', never]],
  Mos,
  T
>;

export type StoreSlice<S, T, Mos extends [StoreMutatorIdentifier, T][] = []> = StateCreator<
  S,
  [['zustand/immer', never]],
  Mos,
  T
>;

export function createStore<T, Mos extends [StoreMutatorIdentifier, T][] = []>(store: Store<T, Mos>) {
  return create<T>()(immer(store));
}
