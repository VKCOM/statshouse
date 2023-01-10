import React from 'react';
import { buildVersion } from '../../common/settings';
import cn from 'classnames';
type BuildVersionProps = {
  className?: string;
};
export function BuildVersion({ className }: BuildVersionProps) {
  return <div className={cn(className)}>{buildVersion}</div>;
}
