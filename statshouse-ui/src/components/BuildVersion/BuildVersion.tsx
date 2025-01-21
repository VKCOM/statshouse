// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { buildVersion } from '@/common/settings';
import cn from 'classnames';
import { appVersionToggle } from '@/components2/AppVersionToggle';

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
