// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback } from 'react';
import { useStatsHouseShallow } from '../../store2';
import { DashboardName } from './DashboardName';
import { DashboardHeader } from './DashboardHeader';
import { Button, ErrorMessages, Tooltip } from '../../components';
import { DashboardVariablesControl } from './DashboardVariablesControl';
import { Link } from 'react-router-dom';
import cn from 'classnames';
import { ReactComponent as SVGCloudArrowUp } from 'bootstrap-icons/icons/cloud-arrow-up.svg';
import { DashboardLayout } from './DashboardLayout';
import { DashboardSettings } from './DashboardSettings';
import { useLinkPlot } from '../../hooks/useLinkPlot';

export type DashboardProps = {
  className?: string;
};

export function _Dashboard({ className }: DashboardProps) {
  const {
    tabNum,
    isEmbed,
    dashboardName,
    tvModeEnable,
    plotsLength,
    variablesLength,
    dashboardLayoutEdit,
    setDashboardLayoutEdit,
    // dashboardLink,
    // dashboardSettingLink,
    isDashboard,
    globalLoadQueries,
    saveDashboard,
  } = useStatsHouseShallow(
    ({
      params: { tabNum, dashboardName, orderPlot, orderVariables, dashboardId },
      isEmbed,
      tvMode: { enable },
      dashboardLayoutEdit,
      setDashboardLayoutEdit,
      // links: { dashboardLink, dashboardSettingLink },
      globalNumQueries,
      saveDashboard,
    }) => ({
      tabNum,
      isEmbed,
      dashboardName,
      tvModeEnable: enable,
      plotsLength: orderPlot.length,
      variablesLength: orderVariables.length,
      dashboardLayoutEdit,
      setDashboardLayoutEdit,
      // dashboardLink,
      // dashboardSettingLink,
      isDashboard: dashboardId != null,
      globalLoadQueries: globalNumQueries > 0,
      saveDashboard,
    })
  );

  const onSaveDashboard = useCallback(() => {
    saveDashboard().then(() => {
      setDashboardLayoutEdit(false);
    });
  }, [saveDashboard, setDashboardLayoutEdit]);

  const dashboardLink = useLinkPlot('-1', true);
  const dashboardSettingLink = useLinkPlot('-2', true);

  const isPlot = +tabNum > -1;

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
              disabled={globalLoadQueries || !dashboardName}
              onClick={onSaveDashboard}
              title={!dashboardName ? 'Required name dashboard' : 'Save dashboard'}
            >
              {globalLoadQueries ? (
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
      <DashboardLayout className={cn('z-10', tabNum === '-1' ? 'position-relative' : 'hidden-dashboard')} />
      {tabNum === '-2' && <DashboardSettings />}
    </div>
  );
}
export const Dashboard = memo(_Dashboard);
