import React, { memo } from 'react';
import { PlotKey } from 'url2';

export type PlotSubMenuProps = {
  className?: string;
  plotKey: PlotKey;
};
export function _PlotSubMenu({ className, plotKey }: PlotSubMenuProps) {
  return <></>;
}
export const PlotSubMenu = memo(_PlotSubMenu);
