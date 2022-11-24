// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

export function oneRequest<T, K>(
  request: (key: K) => Promise<T>,
  cache: Map<K, Promise<T>>,
  key: K,
  liveTime: number = 0
): Promise<T> {
  if (!cache.has(key)) {
    cache.set(
      key,
      request(key).catch((error) => {
        cache.delete(key);
        return Promise.reject(error);
      })
    );
    if (liveTime > 0) {
      setTimeout(() => {
        cache.delete(key);
      }, liveTime);
    }
  }
  return cache.get(key) ?? Promise.reject(new Error('impossible error'));
}

export function createCache<T, K>(): Map<K, Promise<T>> {
  return new Map();
}
