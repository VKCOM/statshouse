import React, { useCallback } from 'react';

import { ReactComponent as SVGChevronDown } from 'bootstrap-icons/icons/chevron-down.svg';
import { ReactComponent as SVGChevronRight } from 'bootstrap-icons/icons/chevron-right.svg';

import css from './style.module.css';
import cn from 'classnames';
import { GroupInfo, GroupKey } from 'url2';

export type DashboardGroupProps = {
  children?: React.ReactNode;
  groupInfo?: GroupInfo;
  toggleShow?: (groupKey: GroupKey) => void;
};

export function DashboardGroup({ children, groupInfo, toggleShow }: DashboardGroupProps) {
  const toggle = useCallback(() => {
    toggleShow?.(groupInfo?.id ?? '');
  }, [groupInfo?.id, toggleShow]);
  if (!groupInfo) {
    return null;
  }
  return (
    <div className={css.dashboardGroup}>
      <div onClick={toggle} className={css.dashboardGroupHeader}>
        <div>{groupInfo.show ? <SVGChevronDown /> : <SVGChevronRight />}</div>
        <div>{groupInfo.name}</div>
        <div>{groupInfo.description}</div>
      </div>
      {groupInfo.show && (
        <div className={cn(css.dashboardGroupList, css['dashboardGroupList_' + groupInfo.size])}>{children}</div>
      )}
    </div>
  );
}
