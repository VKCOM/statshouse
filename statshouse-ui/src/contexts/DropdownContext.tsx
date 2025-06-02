import { createContext } from 'react';
import { emptyFunction } from '@/common/helpers';
import type { SetStateBoolean } from '@/hooks';

export const DropdownContext = createContext<SetStateBoolean>({
  on: emptyFunction,
  off: emptyFunction,
  toggle: emptyFunction,
});
