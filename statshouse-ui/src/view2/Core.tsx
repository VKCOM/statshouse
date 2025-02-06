// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { Outlet } from 'react-router-dom';
import css from './style.module.css';
import { LeftMenu } from '@/components2';
import { useTvModeStore } from '@/store2/tvModeStore';
import { useStatsHouse } from '@/store2';
import { BuildVersion } from '@/components/BuildVersion';

export function Core() {
  const isPlot = useStatsHouse(({ params: { tabNum } }) => +tabNum >= 0);
  const tvModeEnable = useTvModeStore(({ enable }) => enable);
  const isEmbed = useStatsHouse((s) => s.isEmbed);
  return (
    <div className={css.app}>
      <div className={css.left}>{(!tvModeEnable || isPlot) && !isEmbed && <LeftMenu />}</div>
      <div className={css.right}>
        <div className={css.top}></div>
        <div className={css.bottom}>
          <Outlet />
        </div>
        <div hidden={isEmbed}>
          <BuildVersion className="text-end text-secondary build-version container-xl pb-3" />
        </div>
      </div>
    </div>
  );
}
export default Core;
