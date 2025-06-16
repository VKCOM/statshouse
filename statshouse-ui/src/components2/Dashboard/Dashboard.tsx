// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo, useCallback, useMemo } from 'react';
import { useStatsHouse, useStatsHouseShallow } from '@/store2';
import { DashboardName } from './DashboardName';
import { DashboardHeader } from './DashboardHeader';
import { Button, Tooltip } from '@/components/UI';
import { DashboardVariablesControl } from './DashboardVariablesControl';
import { Link } from 'react-router-dom';
import cn from 'classnames';
import { ReactComponent as SVGCloudArrowUp } from 'bootstrap-icons/icons/cloud-arrow-up.svg';
import { DashboardSettings } from './DashboardSettings';
import { useLinkPlot } from '@/hooks/useLinkPlot';
import { useGlobalLoader } from '@/store2/plotQueryStore';
import { useTvModeStore } from '@/store2/tvModeStore';
import { ErrorMessages } from '@/components/ErrorMessages';
import { produce } from 'immer';
import { HistoryList } from '../HistoryList';
import { DashboardGridLayoutHOC } from '@/components2/Dashboard/DashboardGridLayout';
import { selectorOrderPlot } from '@/store2/selectors';

const PATH_VERSION_PARAM = '&dv';

export type DashboardProps = {
  className?: string;
};

export const Dashboard = memo(function Dashboard({ className }: DashboardProps) {
  const globalLoader = useGlobalLoader();
  const plotsLength = useStatsHouse((state) => selectorOrderPlot(state).length);
  const tvModeEnable = useTvModeStore(({ enable }) => enable);
  const {
    tabNum,
    isEmbed,
    dashboardName,
    variablesLength,
    dashboardLayoutEdit,
    setDashboardLayoutEdit,
    isDashboard,
    saveDashboard,
    dashboardId,
    setParams,
    dashboardVersion,
  } = useStatsHouseShallow(
    useCallback(
      ({
        params: { tabNum, dashboardName, orderVariables, dashboardId, dashboardVersion },
        isEmbed,
        dashboardLayoutEdit,
        setDashboardLayoutEdit,
        saveDashboard,
        setParams,
      }) => ({
        tabNum,
        isEmbed,
        dashboardName,
        variablesLength: orderVariables.length,
        dashboardLayoutEdit,
        setDashboardLayoutEdit,
        isDashboard: dashboardId != null,
        saveDashboard,
        dashboardId,
        setParams,
        dashboardVersion,
      }),
      []
    )
  );

  const onSaveDashboard = async () => {
    const dashResponse = await saveDashboard();
    if (dashResponse) {
      setParams(
        produce((params) => {
          params.dashboardCurrentVersion = undefined;
        })
      );
      setDashboardLayoutEdit(false);
    }
  };

  const dashboardLink = useLinkPlot('-1', true);
  const dashboardSettingLink = useLinkPlot('-2', true);
  const dashboardHistoryLink = useLinkPlot('-3', true);

  const isPlot = +tabNum > -1;

  const onVersionClick = useCallback(() => {
    setDashboardLayoutEdit?.(false);
  }, [setDashboardLayoutEdit]);

  const mainPath = useMemo(() => `/view?id=${dashboardId}`, [dashboardId]);

  return (
    <div className={className}>
      {!!dashboardName && !isEmbed && !tvModeEnable && <DashboardName />}
      {!isPlot && plotsLength > 0 && !isEmbed && !tvModeEnable && <DashboardHeader />}
      <ErrorMessages />
      {dashboardLayoutEdit && (
        <ul className="nav nav-tabs mb-4 container-xl">
          <li className="nav-item">
            <Link className={cn('nav-link', tabNum === '-1' && 'active')} to={dashboardLink}>
              Layout
            </Link>
          </li>
          <li className="nav-item">
            <Link className={cn('nav-link', tabNum === '-2' && 'active')} to={dashboardSettingLink}>
              Setting
            </Link>
          </li>
          <li className="nav-item">
            {dashboardId && (
              <Link className={cn('nav-link', tabNum === '-3' && 'active')} to={dashboardHistoryLink}>
                History
              </Link>
            )}
          </li>

          <li className="nav-item flex-grow-1"></li>

          <Tooltip<'li'>
            as="li"
            className="nav-item "
            titleClassName="bg-warning-subtle"
            title={!dashboardName && 'Required name dashboard'}
          >
            <Button
              type="button"
              className="nav-link"
              disabled={globalLoader || !dashboardName}
              onClick={onSaveDashboard}
              title={!dashboardName ? 'Required name dashboard' : 'Save dashboard'}
            >
              {globalLoader ? (
                <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
              ) : (
                <SVGCloudArrowUp className="mx-1" />
              )}
              {isDashboard ? 'Save' : 'Create'}
            </Button>
          </Tooltip>
        </ul>
      )}
      {variablesLength > 0 && tabNum === '-1' && !tvModeEnable && (
        <DashboardVariablesControl className="col-12 container-xl mb-3 z-100 position-relative" />
      )}
      <DashboardGridLayoutHOC className={cn('z-10', tabNum === '-1' ? 'position-relative' : 'hidden-dashboard')} />
      {tabNum === '-2' && <DashboardSettings />}
      {tabNum === '-3' && dashboardId && (
        <HistoryList
          id={dashboardId}
          onVersionClick={onVersionClick}
          mainPath={mainPath}
          pathVersionParam={PATH_VERSION_PARAM}
          currentVersion={dashboardVersion}
        />
      )}
    </div>
  );
});
