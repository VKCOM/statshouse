import React from 'react';
import cn from 'classnames';

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
      <span
        role="button"
        style={{
          display: 'flex',
          padding: '10px 17px',
          textDecoration: 'none',
          whiteSpace: 'nowrap',
          color: 'var(--bs-secondary)',
        }}
      >
        AppVersion {appVersion ? '1' : '2'}
      </span>
    </li>
  );
}
