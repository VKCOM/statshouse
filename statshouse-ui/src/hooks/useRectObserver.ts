// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useCallback, useEffect, useState } from 'react';

function getRect(target: Element, fixed: boolean = false) {
  const nextRect = target.getBoundingClientRect();
  if (!fixed) {
    nextRect.x += window.scrollX;
    nextRect.y += window.scrollY;
  }
  // console.log('getRect', { target, ...nextRect.toJSON() });
  return (r: DOMRect) => {
    if (r.x === nextRect.x && r.y === nextRect.y && r.width === nextRect.width && r.height === nextRect.height) {
      return r;
    }
    return nextRect;
  };
}

export function useRectObserver(
  target?: Element | null,
  fixed: boolean = false,
  active: boolean = true,
  scroll: boolean = true
): [DOMRect, () => void] {
  const [rect, setRect] = useState<DOMRect>(new DOMRect());

  const update = useCallback(() => {
    if (target && active) {
      setRect(getRect(target, fixed));
    }
  }, [active, fixed, target]);

  useEffect(() => {
    if (!target) {
      return;
    }

    update();
    const r = new ResizeObserver(update);
    const m = new MutationObserver(update);
    if (scroll) {
      window.addEventListener('scroll', update, { capture: true });
    }
    r.observe(target, {});
    m.observe(target, { attributes: true });
    return () => {
      if (scroll) {
        window.removeEventListener('scroll', update, { capture: true });
      }
      r.unobserve(target);
      r.disconnect();
      m.disconnect();
    };
  }, [scroll, target, update]);
  return [rect, update];
}
