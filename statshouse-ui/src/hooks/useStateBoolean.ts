import { useMemo, useState } from 'react';

export function useStateBoolean(init: boolean): [boolean, { on: () => void; off: () => void; toggle: () => void }] {
  const [status, setStatus] = useState<boolean>(init);
  const control = useMemo(
    () => ({ on: () => setStatus(true), off: () => setStatus(false), toggle: () => setStatus((s) => !s) }),
    []
  );
  return [status, control];
}
