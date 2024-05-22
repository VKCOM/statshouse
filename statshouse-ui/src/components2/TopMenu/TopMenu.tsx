import React from 'react';

export type TopMenuProps = {
  children?: React.ReactNode;
};
export function TopMenu({ children }: TopMenuProps) {
  return <div>TopMenu {children}</div>;
}
