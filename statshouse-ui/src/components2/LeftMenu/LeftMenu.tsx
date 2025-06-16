// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useRef } from 'react';

import { ReactComponent as SVGLightning } from 'bootstrap-icons/icons/lightning.svg';
import { ReactComponent as SVGGridFill } from 'bootstrap-icons/icons/grid-fill.svg';
import { ReactComponent as SVGPlus } from 'bootstrap-icons/icons/plus.svg';
import { ReactComponent as SVGCardList } from 'bootstrap-icons/icons/card-list.svg';
import { ReactComponent as SVGCpu } from 'bootstrap-icons/icons/cpu.svg';
import { ReactComponent as SVGBrightnessHighFill } from 'bootstrap-icons/icons/brightness-high-fill.svg';
import { ReactComponent as SVGMoonStarsFill } from 'bootstrap-icons/icons/moon-stars-fill.svg';
import { ReactComponent as SVGCircleHalf } from 'bootstrap-icons/icons/circle-half.svg';
import { ReactComponent as SVGGear } from 'bootstrap-icons/icons/gear.svg';
import css from './style.module.css';
import { LeftMenuItem } from './LeftMenuItem';
import { Link, NavLink, useLocation } from 'react-router-dom';
import { globalSettings } from '@/common/settings';
import cn from 'classnames';
import { getClipboard } from '@/common/helpers';
import { useStatsHouse, useStatsHouseShallow } from '@/store2';
import { addPlotByUrl } from '@/store2/helpers';
import { produce } from 'immer';
import { LeftMenuPlotItem } from './LeftMenuPlotItem';
import { useAddLinkPlot, useLinkPlot } from '@/hooks/useLinkPlot';
import { setDevEnabled, useStoreDev } from '@/store2/dev';
import { setTheme, THEMES, toTheme, useThemeStore } from '@/store2/themeStore';
import { WidgetPlotContextProvider } from '@/contexts/WidgetPlotContextProvider';
import { selectorOrderPlotShow } from '@/store2/selectors';

const themeIcon = {
  [THEMES.Light]: SVGBrightnessHighFill,
  [THEMES.Dark]: SVGMoonStarsFill,
  [THEMES.Auto]: SVGCircleHalf,
};

const toggleDevEnabled = () => {
  setDevEnabled((s) => !s);
};

export type LeftMenuProps = {
  className?: string;
};
export function LeftMenu({ className }: LeftMenuProps) {
  const location = useLocation();
  const devEnabled = useStoreDev((s) => s.enabled);
  const theme = useThemeStore((s) => s.theme);
  const { tabNum, setUrlStore, user, paramsTheme } = useStatsHouseShallow(
    ({ params: { theme, tabNum }, setUrlStore, user }) => ({
      tabNum,
      setUrlStore,
      user,
      paramsTheme: theme,
    })
  );
  const viewPlots = useStatsHouse(selectorOrderPlotShow);

  const isView = location.pathname.indexOf('view') > -1;
  const isSettings = location.pathname.indexOf('settings') > -1;
  const isDash = tabNum === '-1' || tabNum === '-2' || tabNum === '-3';
  const onSetTheme = useCallback((event: React.MouseEvent) => {
    const value = toTheme(event.currentTarget.getAttribute('data-value'));
    if (value) {
      setTheme(value);
    }
  }, []);
  const refListMenuItemPlot = useRef<HTMLUListElement>(null);

  const dashboardLink = useLinkPlot('-1', true);
  const addLink = useAddLinkPlot(true);

  useEffect(() => {
    setTimeout(() => {
      refListMenuItemPlot.current?.querySelector('.' + css.active)?.scrollIntoView({ block: 'nearest' });
    }, 0);
  }, [viewPlots]);

  const onPasteClipboard = useCallback(() => {
    getClipboard().then((url) => {
      setUrlStore((state) => {
        state.params = addPlotByUrl(url, state.params);
      });
    });
  }, [setUrlStore]);
  const onResetTheme = useCallback(() => {
    useStatsHouse.getState().setParams(
      produce((p) => {
        p.theme = undefined;
      })
    );
  }, []);
  return (
    <ul className={cn(css.leftMenu, className)}>
      <LeftMenuItem to="view" icon={SVGLightning} reloadDocument title="StatsHouse">
        <li className={css.splitter}></li>
        <li className={css.subItem}>
          <NavLink className={css.link} to="/admin/create" end>
            Create&nbsp;metric
          </NavLink>
        </li>
        <li className={css.splitter}></li>
        <li className={css.subItem}>
          <a className={css.link} href="https://vkcom.github.io/statshouse/" target="_blank" rel="noreferrer">
            Documentation
          </a>
        </li>
        <li className={css.subItem}>
          <a className={css.link} href="/openapi/" target="_blank">
            OpenAPI
          </a>
        </li>
        <li className={css.subItem}>
          <Link className={css.link} to="/doc/faq">
            FAQ
          </Link>
        </li>
        {!!globalSettings.links?.length && <li className={css.splitter}></li>}
        {!!globalSettings.links?.length &&
          globalSettings.links.map(({ url, name }, index) => (
            <li key={index} className={css.subItem}>
              <Link className={css.link} target="_blank" to={url}>
                {name}
              </Link>
            </li>
          ))}
        {user.developer && (
          <>
            <li className={css.splitter}></li>
            <li className={cn(css.subItem, devEnabled && 'bg-primary-subtle')} onClick={toggleDevEnabled}>
              <span role="button" className={css.link}>
                {devEnabled ? 'DEV ON' : 'DEV OFF'}
              </span>
            </li>
            <li className={css.subItem}>
              <NavLink className={css.link} to="/admin/dash" end>
                Admin Dashboard List
              </NavLink>
            </li>
          </>
        )}
        {!!user.login && (
          <>
            <li className={css.splitter}></li>
            <li className={css.subItem}>
              <span className={css.link}>{user.login}</span>
            </li>
            <li className={css.subItem}>
              <a className={css.link} href={user.logoutURL} title="Log out">
                Log&nbsp;out
              </a>
            </li>
          </>
        )}
      </LeftMenuItem>
      <LeftMenuItem icon={themeIcon[theme] ?? SVGBrightnessHighFill} title="Theme">
        <li className={css.splitter}></li>
        {!!paramsTheme && (
          <>
            <li className={cn(css.subItem, ' bg-primary-subtle')}>
              <span role="button" className={css.link} title="Reset url theme" onClick={onResetTheme}>
                Reset
              </span>
            </li>
            <li className={css.splitter}></li>
          </>
        )}
        <li className={cn(css.subItem, theme === THEMES.Dark && 'bg-primary-subtle')}>
          <span role="button" className={css.link} title="Set dark theme" data-value={THEMES.Dark} onClick={onSetTheme}>
            Dark
          </span>
        </li>
        <li className={cn(css.subItem, theme === THEMES.Light && 'bg-primary-subtle')}>
          <span
            role="button"
            className={css.link}
            title="Set light theme"
            data-value={THEMES.Light}
            onClick={onSetTheme}
          >
            Light
          </span>
        </li>
        <li className={cn(css.subItem, theme === THEMES.Auto && 'bg-primary-subtle')}>
          <span role="button" className={css.link} title="Set auto theme" data-value={THEMES.Auto} onClick={onSetTheme}>
            Auto
          </span>
        </li>
      </LeftMenuItem>
      {user.admin && (
        <LeftMenuItem icon={SVGGear} to="settings/group" title="Group" active={isSettings}>
          <li className={css.splitter}></li>
          <li className={css.subItem}>
            <Link className={css.link} to="settings/namespace">
              Namespace
            </Link>
          </li>
        </LeftMenuItem>
      )}
      {!!globalSettings.admin_dash && (
        <LeftMenuItem icon={SVGCpu} to={`view?id=${globalSettings.admin_dash}`} title="Hardware info"></LeftMenuItem>
      )}
      <LeftMenuItem
        icon={SVGCardList}
        to="dash-list"
        active={location.pathname.indexOf('dash-list') > -1}
        title="Dashboard list"
      ></LeftMenuItem>
      <LeftMenuItem icon={SVGGridFill} to={dashboardLink} active={isView && isDash} title="Dashboard"></LeftMenuItem>
      <li className={cn(css.scrollStyle, css.plotMenu)}>
        <ul ref={refListMenuItemPlot} className={cn(css.plotNav)}>
          {viewPlots.map((plotKey) => (
            <WidgetPlotContextProvider key={plotKey} plotKey={plotKey}>
              <LeftMenuPlotItem key={plotKey} active={isView && tabNum === plotKey} />
            </WidgetPlotContextProvider>
          ))}
        </ul>
      </li>
      <LeftMenuItem icon={SVGPlus} title="Duplicate plot to new tab" to={addLink}>
        <li className={css.splitter}></li>
        <li className={css.subItem}>
          <span role="button" className={css.link} title="Paste new tab from clipboard" onClick={onPasteClipboard}>
            Paste&nbsp;Clipboard
          </span>
        </li>
      </LeftMenuItem>
    </ul>
  );
}
