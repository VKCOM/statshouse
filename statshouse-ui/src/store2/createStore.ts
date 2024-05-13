import { create, StateCreator, StoreMutatorIdentifier } from 'zustand';
import { devtools } from 'zustand/middleware';

export function createStore<T, Mos extends [StoreMutatorIdentifier, T][] = []>(
  store: StateCreator<T, [], Mos, T>,
  name: string = ''
) {
  // if (process.env.NODE_ENV === 'development') {
  return create<T>()(
    devtools(store, {
      name: store.name || name,
      trace: true,
      store: store.name || name,
      anonymousActionType: 'setState',
      enabled: process.env.NODE_ENV === 'development',
    })
  );
  // }
  // return create<T>()(store);
}
