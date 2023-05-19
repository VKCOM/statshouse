import { useEffect, useState } from 'react';

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

export function useRectObserver(target?: Element | null, fixed: boolean = false) {
  const [rect, setRect] = useState<DOMRect>(new DOMRect());
  useEffect(() => {
    if (!target) {
      return;
    }
    const upd = () => {
      setRect(getRect(target, fixed));
    };
    upd();
    const r = new ResizeObserver(upd);
    const m = new MutationObserver(upd);
    window.addEventListener('scroll', upd, { capture: true });
    r.observe(target, {});
    m.observe(target, { attributes: true });
    return () => {
      window.removeEventListener('scroll', upd, { capture: true });
      r.unobserve(target);
      r.disconnect();
      m.disconnect();
    };
  }, [fixed, target]);
  return rect;
}
