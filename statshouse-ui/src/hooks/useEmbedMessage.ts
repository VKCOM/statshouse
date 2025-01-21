// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useRectObserver } from './useRectObserver';
import { useEffect } from 'react';

export function useEmbedMessage(refPage: Element | null, embed?: boolean) {
  const [{ width, height }] = useRectObserver(refPage, true, embed, false);
  useEffect(() => {
    if (embed) {
      window.top?.postMessage({ source: 'statshouse', payload: { width, height } }, '*');
    }
  }, [embed, height, width]);
}
