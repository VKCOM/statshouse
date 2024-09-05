import React from 'react';

import { Outlet } from 'react-router-dom';
import 'store2';
import css from './style.module.css';
import { LeftMenu } from 'components2';
import { useTvModeStore } from 'store2/tvModeStore';
import { useStatsHouse } from '../store2';

export function Core() {
  const tvModeEnable = useTvModeStore(({ enable }) => enable);
  const isEmbed = useStatsHouse((s) => s.isEmbed);
  return (
    <div className={css.app}>
      <div className={css.left}>{!tvModeEnable && !isEmbed && <LeftMenu />}</div>
      <div className={css.right}>
        <div className={css.top}>{/*<TopMenuWidget />*/}</div>
        <div className={css.bottom}>
          <Outlet />
        </div>
      </div>
    </div>
  );
}
export default Core;
