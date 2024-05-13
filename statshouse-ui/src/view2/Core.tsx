import React from 'react';

import { Outlet } from 'react-router-dom';
import '../store2';

import css from './style.module.css';
import { LeftMenuWidget } from '../widgets2';

export function Core() {
  return (
    <div className={css.app}>
      <div className={css.left}>
        <LeftMenuWidget />
      </div>
      <div className={css.right}>
        <div className={css.top}></div>
        <div className={css.bottom}>
          <Outlet />
        </div>
      </div>
    </div>
  );
}
export default Core;
