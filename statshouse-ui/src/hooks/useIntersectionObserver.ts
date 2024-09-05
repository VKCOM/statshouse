import { RefObject, useEffect, useState } from 'react';

export function buildThresholdList(step: number = 0.5) {
  let thresholds = [];
  for (let i = 0; i <= 1; i += step) {
    thresholds.push(i);
  }
  return thresholds;
}

export function useIntersectionObserver(
  target?: Element | null,
  threshold: number | number[] = 0.5,
  root?: RefObject<Element | null | undefined>,
  initVisible: number = 1
) {
  const [visible, setVisible] = useState<number>(initVisible);

  useEffect(() => {
    if (!target || target instanceof SVGElement) {
      return;
    }
    const upd = (entries: IntersectionObserverEntry[]) => {
      setVisible(entries.reduce((res, e) => e.intersectionRatio, 0));
    };
    const o = new IntersectionObserver(upd, { threshold });
    o.observe(target);
    return () => {
      o.unobserve(target);
      o.disconnect();
      setVisible(initVisible);
    };
  }, [initVisible, root, target, threshold]);
  return visible;
}
