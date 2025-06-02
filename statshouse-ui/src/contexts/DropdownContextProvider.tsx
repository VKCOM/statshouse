import type { ReactNode } from 'react';
import type { SetStateBoolean } from '@/hooks';
import { DropdownContext } from '@/contexts/DropdownContext';

export type DropdownContextProviderProps = {
  children?: ReactNode;
  value: SetStateBoolean;
};

export function DropdownContextProvider({ children, value }: DropdownContextProviderProps) {
  return <DropdownContext.Provider value={value}>{children}</DropdownContext.Provider>;
}
