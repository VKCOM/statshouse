// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { createCache, oneRequest } from './oneRequest';

describe('oneRequest', () => {
  test('return cache', async () => {
    const c = createCache();
    expect(await oneRequest((key) => Promise.resolve(key + ' ' + key), c, 'test')).toBe('test test');
    expect(await oneRequest((key) => Promise.resolve(key + ' no ' + key), c, 'test')).toBe('test test');
  });
  test('return error', async () => {
    const c = createCache();
    expect(await oneRequest((key) => Promise.reject(key + ' ok ' + key), c, 'test').catch(() => 'error')).toBe('error');
    expect(await oneRequest((key) => Promise.resolve(key + ' ok ' + key), c, 'test').catch(() => 'error')).toBe(
      'test ok test'
    );
    expect(await oneRequest((key) => Promise.reject(key + ' ok ' + key), c, 'test').catch(() => 'error')).toBe(
      'test ok test'
    );
  });
});
