import { RefObject, useEffect, useState } from 'react';

function getRect(target: Element, fixed: boolean = false) {
  let nextRect = target.getBoundingClientRect();
  if (!fixed) {
    nextRect.x += window.scrollX;
    nextRect.y += window.scrollY;
  }
  return (r: DOMRect) => {
    if (r.x === nextRect.x && r.y === nextRect.y && r.width === nextRect.width && r.height === nextRect.height) {
      return r;
    }
    return nextRect;
  };
}

export function useRectObserver(ref?: RefObject<Element | null | undefined>, fixed: boolean = false) {
  const [rect, setRect] = useState<DOMRect>(new DOMRect());
  const target = ref?.current;

  useEffect(() => {
    if (!target) {
      return;
    }
    const upd = () => {
      setRect(getRect(target, fixed));
    };
    upd();
    const o = new ResizeObserver(upd);
    window.addEventListener('scroll', upd, { capture: true });
    o.observe(target);
    return () => {
      window.removeEventListener('scroll', upd, { capture: true });
      o.unobserve(target);
      o.disconnect();
    };
  }, [fixed, target]);
  return rect;
}
