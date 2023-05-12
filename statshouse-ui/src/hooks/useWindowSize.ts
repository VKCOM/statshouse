import { useEffect, useMemo, useState } from 'react';

export function useWindowSize() {
  const [height, setHeight] = useState(0);
  const [width, setWidth] = useState(0);
  const [scrollX, setScrollX] = useState(0);
  const [scrollY, setScrollY] = useState(0);
  useEffect(() => {
    const upd = () => {
      setWidth(window.innerWidth);
      setHeight(window.innerHeight);
      setScrollX(window.scrollX);
      setScrollY(window.scrollY);
    };
    upd();
    window.addEventListener('resize', upd, false);
    window.addEventListener('scroll', upd, false);
    return () => {
      window.removeEventListener('resize', upd, false);
      window.removeEventListener('scroll', upd, false);
    };
  }, []);
  return useMemo(() => new DOMRect(scrollX, scrollY, width, height), [height, scrollX, scrollY, width]);
}
