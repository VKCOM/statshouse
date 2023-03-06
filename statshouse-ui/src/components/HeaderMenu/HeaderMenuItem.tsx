// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useRef, useState } from 'react';
import { To } from 'react-router-dom';
import cn from 'classnames';

import css from './style.module.css';
import { PlotLink } from '../Plot/PlotLink';

export type HeaderMenuItemProps = {
  children?: React.ReactNode;
  icon: React.FC<{ className?: string }>;
  title: string;
  description?: React.ReactNode;
  to?: To;
  indexPlot?: number;
  className?: string;
};

export const HeaderMenuItem: React.FC<HeaderMenuItemProps> = ({
  children,
  icon: Icon,
  title,
  indexPlot,
  to,
  description,
  className,
}) => {
  const touchToggle = useRef<HTMLAnchorElement>(null);
  const sub = useRef<HTMLUListElement>(null);
  const [open, setOpen] = useState(false);
  const openRef = useRef(open);

  const onOpen = useCallback(() => {
    setOpen(true);
  }, []);

  const onClose = useCallback(() => {
    setOpen(false);
  }, []);
  const onToggle = useCallback(() => {
    setOpen((s) => !s);
  }, []);

  useEffect(() => {
    openRef.current = open;
  }, [open]);

  useEffect(() => {
    const onTouchToggle = (event: Event) => {
      let t = event.target as HTMLElement;
      while (t.parentElement && !(t === touchToggle.current || t === sub.current)) {
        t = t.parentElement;
      }
      if (t === touchToggle.current) {
        if (!openRef.current) {
          setOpen((s) => !s);
          event.preventDefault();
        }
      } else if (t !== sub.current) {
        setOpen(false);
      }
    };
    document.addEventListener('touchstart', onTouchToggle, { passive: false });
    return () => {
      document.removeEventListener('touchstart', onTouchToggle);
    };
  }, []);

  return (
    <li className={cn('position-relative', className)} onMouseOver={onOpen} onMouseOut={onClose} onClick={onClose}>
      {to || indexPlot !== undefined ? (
        <PlotLink className="nav-link" to={to} indexPlot={indexPlot} title={title} ref={touchToggle}>
          <Icon className={css.icon} />
        </PlotLink>
      ) : (
        <span className={cn('nav-link text-secondary')} onTouchStart={onToggle}>
          <Icon className={css.icon} />
        </span>
      )}

      <ul
        hidden={!open}
        className={cn(`nav d-flex flex-column position-absolute start-100 top-0`, css.nav, css.sub)}
        ref={sub}
      >
        {to || indexPlot !== undefined ? (
          <li className="nav-item">
            <PlotLink className="nav-link text-nowrap" to={to} indexPlot={indexPlot} title={title}>
              {description ?? title}
            </PlotLink>
          </li>
        ) : (
          <li className="nav-item">
            <span className={cn('nav-link text-secondary text-nowrap')}>{description ?? title}</span>
          </li>
        )}
        {children}
      </ul>
    </li>
  );
};
