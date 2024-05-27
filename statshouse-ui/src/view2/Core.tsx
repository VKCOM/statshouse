import React from 'react';

import { Outlet } from 'react-router-dom';
import { LeftMenuWidget, TopMenuWidget } from 'widgets2';
import '../store2';
import css from './style.module.css';

export function Core() {
  return (
    <div className={css.app}>
      <div className={css.left}>
        <LeftMenuWidget />
      </div>
      <div className={css.right}>
        <div className={css.top}>
          <TopMenuWidget />
        </div>
        <div className={css.bottom}>
          <Outlet />
        </div>
      </div>
    </div>
  );
}
export default Core;
