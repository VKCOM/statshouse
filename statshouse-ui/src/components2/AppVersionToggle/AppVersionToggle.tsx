import React from 'react';
import cn from 'classnames';

import css from './style.module.css';
const appVersion = localStorage.getItem('appVersion');
export function appVersionToggle() {
  if (appVersion) {
    localStorage.removeItem('appVersion');
  } else {
    localStorage.setItem('appVersion', '1');
  }
  document.location.reload();
}

export function AppVersionToggle() {
  return (
    <li className={cn()} onClick={appVersionToggle}>
      <span role="button" className={css.link}>
        AppVersion {appVersion ? '1' : '2'}
      </span>
    </li>
  );
}
