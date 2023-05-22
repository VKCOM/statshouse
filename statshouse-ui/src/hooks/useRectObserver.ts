import { useCallback, useEffect, useState } from 'react';

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

export function useRectObserver(target?: Element | null, fixed: boolean = false): [DOMRect, () => void] {
  const [rect, setRect] = useState<DOMRect>(new DOMRect());

  const update = useCallback(() => {
    if (target) {
      setRect(getRect(target, fixed));
    }
  }, [fixed, target]);

  useEffect(() => {
    if (!target) {
      return;
    }

    update();
    const r = new ResizeObserver(update);
    const m = new MutationObserver(update);
    window.addEventListener('scroll', update, { capture: true });
    r.observe(target, {});
    m.observe(target, { attributes: true });
    return () => {
      window.removeEventListener('scroll', update, { capture: true });
      r.unobserve(target);
      r.disconnect();
      m.disconnect();
    };
  }, [target, update]);
  return [rect, update];
}
