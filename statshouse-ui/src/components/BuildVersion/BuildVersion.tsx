import React from 'react';
import { buildVersion } from 'common/settings';
import cn from 'classnames';
import { appVersionToggle } from 'components2/AppVersionToggle';
type BuildVersionProps = {
  className?: string;
  prefix?: string;
};
export function BuildVersion({ className, prefix }: BuildVersionProps) {
  return (
    <div className={cn(className)}>
      {!!prefix && <span onDoubleClick={appVersionToggle}>{prefix}</span>}
      <span>{buildVersion}</span>
    </div>
  );
}
