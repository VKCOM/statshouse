// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { RefObject, useEffect } from 'react';

export function useOnClickOutside(
  ref: RefObject<Element | null> | RefObject<Element | null>[],
  callback?: (event: MouseEvent) => void
) {
  useEffect(() => {
    const on = (event: MouseEvent) => {
      const refs = Array.isArray(ref) ? ref : [ref];
      const contains = refs.some(
        (r) =>
          r.current &&
          (r.current === event.target || (event.target instanceof Element && r.current.contains(event.target)))
      );
      if (!contains) {
        callback?.(event);
      }
    };
    window.document.addEventListener('click', on);
    return () => {
      window.document.removeEventListener('click', on);
    };
  }, [ref, callback]);
}
