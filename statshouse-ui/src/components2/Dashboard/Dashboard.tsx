import React from 'react';
import css from './style.module.css';

export type DashboardProps = {
  children?: React.ReactNode;
};

export function Dashboard({ children }: DashboardProps) {
  return <div className={css.dashboard}>{children}</div>;
}
