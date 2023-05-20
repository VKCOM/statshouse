import { RefObject, useEffect } from 'react';

export function useOnClickOutside(ref: RefObject<Element>, callback?: (event: MouseEvent) => void) {
  useEffect(() => {
    const on = (event: MouseEvent) => {
      if (ref.current && !(event.target instanceof Node && ref.current.contains(event.target))) {
        callback?.(event);
      }
    };
    document.addEventListener('click', on);
    return () => {
      document.removeEventListener('click', on);
    };
  }, [ref, callback]);
}
