// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

const cache = new Map<any, any>();

export function promiseRun<F extends (...args: any[]) => any>(
  keyPromise: any,
  promiseCallback: F,
  ...args: Parameters<F>
): [ReturnType<F>, boolean] {
  let promise: ReturnType<F> = cache.get(keyPromise);
  let first = false;
  if (!promise) {
    first = true;
    promise = promiseCallback.apply(undefined, args);
    cache.set(keyPromise, promise);
    promise.finally(() => {
      cache.delete(keyPromise);
    });
  }
  return [promise, first];
}
