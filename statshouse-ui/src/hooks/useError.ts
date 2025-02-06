// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { ErrorStore, rootErrorChannel, useErrorStore } from '@/store2/errors';
import { useCallback } from 'react';

export function useError(channel: string = rootErrorChannel) {
  const selector = useCallback((s: ErrorStore) => s.errors[channel], [channel]);
  return useErrorStore(selector) ?? [];
}
