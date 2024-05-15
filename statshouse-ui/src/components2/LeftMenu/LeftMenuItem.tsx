import React, { memo, useCallback, useRef } from 'react';
import { Link, To } from 'react-router-dom';

import { useOnClickOutside, useStateBoolean, useStateToRef } from 'hooks';
import { Popper } from 'components';

import css from './style.module.css';
import cn from 'classnames';

export type LeftMenuItemProps = {
  icon: React.FC<{ className?: string }>;
  title?: React.ReactNode;
  to?: To;
  children?: React.ReactNode;
  active?: boolean;
  className?: string;
};
export function _LeftMenuItem({ children, icon: Icon, title, to, active = false, className }: LeftMenuItemProps) {
  const itemRef = useRef(null);
  const sub = useRef<HTMLUListElement>(null);
  const [open, setOpen] = useStateBoolean(false);
  const openRef = useStateToRef(open);
  useOnClickOutside(itemRef, setOpen.off);

  const onClick = useCallback(
    (event: React.MouseEvent) => {
      if (!openRef.current) {
        setOpen.on();
        event.preventDefault();
        event.stopPropagation();
      }
    },
    [openRef, setOpen]
  );

  return (
    <li
      className={cn(className, css.leftMenuItem, active && css.active)}
      ref={itemRef}
      onMouseOver={setOpen.on}
      onMouseOut={setOpen.off}
      onClick={setOpen.off}
    >
      {to != null ? (
        <Link className={css.link} to={to} onClick={onClick}>
          <Icon className={css.icon} />
        </Link>
      ) : (
        <span className={css.link} onClick={onClick}>
          <Icon className={css.icon} />
        </span>
      )}
      <Popper targetRef={itemRef} fixed={false} horizontal={'out-right'} vertical={'top'} show={open} always>
        <ul className={css.sub} ref={sub}>
          <li className={css.subItem}>
            {to != null ? (
              <Link className={css.link} to={to}>
                {title}
              </Link>
            ) : (
              <span className={css.link}>{title}</span>
            )}
          </li>
          {children}
        </ul>
      </Popper>
    </li>
  );
}
export const LeftMenuItem = memo(_LeftMenuItem);
