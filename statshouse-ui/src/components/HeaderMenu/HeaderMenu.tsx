// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useRef } from 'react';
import { NavLink, useLocation } from 'react-router-dom';
import { ReactComponent as SVGLightning } from 'bootstrap-icons/icons/lightning.svg';
import { ReactComponent as SVGGridFill } from 'bootstrap-icons/icons/grid-fill.svg';
import { ReactComponent as SVGPlus } from 'bootstrap-icons/icons/plus.svg';
import { ReactComponent as SVGCardList } from 'bootstrap-icons/icons/card-list.svg';
import { ReactComponent as SVGBrightnessHighFill } from 'bootstrap-icons/icons/brightness-high-fill.svg';
import { ReactComponent as SVGMoonStarsFill } from 'bootstrap-icons/icons/moon-stars-fill.svg';
import { ReactComponent as SVGCircleHalf } from 'bootstrap-icons/icons/circle-half.svg';
import { ReactComponent as SVGLightbulbFill } from 'bootstrap-icons/icons/lightbulb-fill.svg';
import { ReactComponent as SVGGear } from 'bootstrap-icons/icons/gear.svg';
import cn from 'classnames';
import { produce } from 'immer';

import { HeaderMenuItem } from './HeaderMenuItem';
import {
  selectorDevEnabled,
  selectorParams,
  selectorPlotList,
  selectorPromqltestfailed,
  selectorSetParams,
  setDevEnabled,
  setTheme,
  THEMES,
  useStore,
  useStoreDev,
  useThemeStore,
} from '../../store';
import { currentAccessInfo, logoutURL } from '../../common/access';
import { HeaderMenuItemPlot } from './HeaderMenuItemPlot';
import css from './style.module.css';
import { decodeParams } from '../../url/queryParams';

const themeIcon = {
  [THEMES.Light]: SVGBrightnessHighFill,
  [THEMES.Dark]: SVGMoonStarsFill,
  [THEMES.Auto]: SVGCircleHalf,
};

export type HeaderMenuProps = {
  className?: string;
};

const toggleDevEnabled = () => {
  setDevEnabled((s) => !s);
};

export const HeaderMenu: React.FC<HeaderMenuProps> = ({ className }) => {
  const params = useStore(selectorParams);
  const setParams = useStore(selectorSetParams);
  const menuPlots = useStore(selectorPlotList);
  const devEnabled = useStoreDev(selectorDevEnabled);
  const location = useLocation();
  const ai = currentAccessInfo();

  const themeName = useThemeStore((s) => s.theme);

  const promqltestfailed = useStore(selectorPromqltestfailed);

  const isView = location.pathname === '/view';
  const isDashList = location.pathname === '/dash-list';
  const isSettings = location.pathname === '/settings' || location.pathname === '/settings/group';

  const onPasteClipboard = useCallback(() => {
    (navigator.clipboard.readText ? navigator.clipboard.readText() : Promise.reject())
      .then((url) => {
        const parseParams = decodeParams([...new URLSearchParams(url).entries()]);
        if (parseParams.plots.length) {
          setParams(
            produce((p) => {
              p.plots = [...p.plots, ...parseParams.plots];
              if (p.dashboard?.groupInfo?.length) {
                p.dashboard.groupInfo[p.dashboard.groupInfo.length - 1].count++;
              }
            })
          );
        }
      })
      .catch(() => {
        const url = prompt('Paste url') ?? '';
        const parseParams = decodeParams([...new URLSearchParams(url).entries()]);
        if (parseParams.plots.length) {
          setParams(
            produce((p) => {
              p.plots = [...p.plots, ...parseParams.plots];
              if (p.dashboard?.groupInfo?.length) {
                p.dashboard.groupInfo[p.dashboard.groupInfo.length - 1].count++;
              }
            })
          );
        }
      });
  }, [setParams]);

  const onDark = useCallback(() => {
    setTheme(THEMES.Dark);
  }, []);
  const onLight = useCallback(() => {
    setTheme(THEMES.Light);
  }, []);
  const onAuto = useCallback(() => {
    setTheme(THEMES.Auto);
  }, []);
  const onResetTheme = useCallback(() => {
    useStore.getState().setParams(
      produce((p) => {
        p.theme = undefined;
      })
    );
  }, []);
  const refListMenuItemPlot = useRef<HTMLUListElement>(null);

  useEffect(() => {
    setTimeout(() => {
      refListMenuItemPlot.current?.querySelector('.plot-active')?.scrollIntoView();
    }, 0);
  }, []);

  return (
    <div className={cn('sticky-top align-self-start', css.navOuter, className)}>
      <ul className={cn('nav pb-2 h-100 d-flex flex-column flex-nowrap ', css.nav)}>
        <HeaderMenuItem icon={SVGLightning} title="Home" to="/view" description="StatsHouse">
          <li className={css.splitter}></li>
          <li className="nav-item">
            <NavLink className="nav-link" to="/admin/create" end>
              Create&nbsp;metric
            </NavLink>
          </li>
          <li className={css.splitter}></li>
          <li className="nav-item">
            <a
              className="nav-link"
              href="https://github.com/VKCOM/statshouse/discussions/categories/announcements"
              target="_blank"
              rel="noreferrer"
            >
              News
            </a>
          </li>
          <li className="nav-item">
            <a
              className="nav-link"
              href="https://github.com/VKCOM/statshouse#documentation"
              target="_blank"
              rel="noreferrer"
            >
              Documentation
            </a>
          </li>
          <li className="nav-item">
            <a className="nav-link" href="/openapi/" target="_blank">
              OpenAPI
            </a>
          </li>
          <li className="nav-item">
            <a
              className="nav-link"
              href="https://github.com/VKCOM/statshouse/discussions/categories/q-a"
              target="_blank"
              rel="noreferrer"
            >
              Support
            </a>
          </li>
          <li className="nav-item">
            <NavLink className="nav-link" to="/doc/faq" end>
              FAQ
            </NavLink>
          </li>
          {ai.developer && (
            <>
              <li className={css.splitter}></li>
              <li className={cn('nav-item', devEnabled && 'bg-primary-subtle')} onClick={toggleDevEnabled}>
                <span role="button" className="nav-link">
                  {devEnabled ? 'DEV ON' : 'DEV OFF'}
                </span>
              </li>
            </>
          )}
          {!!ai.user && (
            <>
              <li className={css.splitter}></li>
              <li className="nav-item">
                <span className="nav-link text-secondary">{ai.user}</span>
              </li>
              <li className="nav-item">
                <a className="nav-link" href={logoutURL} title="Log out">
                  Log&nbsp;out
                </a>
              </li>
            </>
          )}
        </HeaderMenuItem>
        <HeaderMenuItem icon={themeIcon[themeName] ?? SVGBrightnessHighFill} title="Theme">
          <li className={css.splitter}></li>
          {!!params.theme && (
            <>
              <li className={cn('nav-item bg-primary-subtle')}>
                <span role="button" className="nav-link" title="Reset url theme" onClick={onResetTheme}>
                  Reset
                </span>
              </li>
              <li className={css.splitter}></li>
            </>
          )}
          <li className={cn('nav-item', themeName === THEMES.Dark && 'bg-primary-subtle')}>
            <span role="button" className="nav-link" title="Set dark theme" onClick={onDark}>
              Dark
            </span>
          </li>
          <li className={cn('nav-item', themeName === THEMES.Light && 'bg-primary-subtle')}>
            <span role="button" className="nav-link" title="Set light theme" onClick={onLight}>
              Light
            </span>
          </li>
          <li className={cn('nav-item', themeName === THEMES.Auto && 'bg-primary-subtle')}>
            <span role="button" className="nav-link" title="Set auto theme" onClick={onAuto}>
              Auto
            </span>
          </li>
        </HeaderMenuItem>
        {ai.admin && (
          <HeaderMenuItem
            icon={SVGGear}
            to="/settings/group"
            title="Group"
            className={cn(isSettings && css.activeItem)}
          >
            <li className={css.splitter}></li>
            <li className="nav-item">
              <NavLink className="nav-link" to="/settings/namespace" end>
                Namespace
              </NavLink>
            </li>
          </HeaderMenuItem>
        )}
        <HeaderMenuItem
          icon={SVGCardList}
          to="/dash-list"
          title="Dashboard list"
          className={cn(isDashList && css.activeItem)}
        ></HeaderMenuItem>
        <HeaderMenuItem
          icon={SVGGridFill}
          indexPlot={-1}
          title="Dashboard"
          className={cn(params.tabNum < 0 && isView && css.activeItem)}
        ></HeaderMenuItem>
        <li className={cn('flex-grow-0 w-100 overflow-auto', css.scrollStyle)}>
          <ul ref={refListMenuItemPlot} className={cn('nav d-flex flex-column', css.nav)}>
            {menuPlots.map((item) => (
              <HeaderMenuItemPlot key={item.indexPlot} indexPlot={item.indexPlot} />
            ))}
          </ul>
        </li>
        <HeaderMenuItem icon={SVGPlus} indexPlot={params.plots.length} title="Duplicate plot to new tab">
          <li className={css.splitter}></li>
          <li className="nav-item">
            <span role="button" className="nav-link" title="Paste new tab from clipboard" onClick={onPasteClipboard}>
              Paste&nbsp;Clipboard
            </span>
          </li>
        </HeaderMenuItem>
        {ai.developer && devEnabled && promqltestfailed && (
          <HeaderMenuItem
            icon={SVGLightbulbFill}
            title="promqltestfailed"
            className={css.secondDanger}
          ></HeaderMenuItem>
        )}
      </ul>
    </div>
  );
};
