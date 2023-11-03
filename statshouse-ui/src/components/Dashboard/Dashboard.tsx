// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback } from 'react';
import { ReactComponent as SVGCloudArrowUp } from 'bootstrap-icons/icons/cloud-arrow-up.svg';

import {
  selectorDashboardLayoutEdit,
  selectorGlobalNumQueriesPlot,
  selectorIsServer,
  selectorParams,
  selectorParamsTabNum,
  selectorSaveServerParams,
  selectorSetDashboardLayoutEdit,
  useStore,
} from '../../store';
import { DashboardHeader } from './DashboardHeader';
import { DashboardLayout } from './DashboardLayout';
import { DashboardSettings } from '../DashboardSettings';
import cn from 'classnames';
import { PlotLink } from '../Plot/PlotLink';
import { ErrorMessages } from '../ErrorMessages';
import { DashboardVariablesControl } from './DashboardVariablesControl';
import { Button, Tooltip } from '../UI';

export type DashboardProps = {
  yAxisSize?: number;
  embed?: boolean;
};

export const Dashboard: React.FC<DashboardProps> = ({ embed = false, yAxisSize = 54 }) => {
  const params = useStore(selectorParams);
  const numQueries = useStore(selectorGlobalNumQueriesPlot);

  const dashboardLayoutEdit = useStore(selectorDashboardLayoutEdit);
  const setDashboardLayoutEdit = useStore(selectorSetDashboardLayoutEdit);
  const saveServerParams = useStore(selectorSaveServerParams);

  const tabNum = useStore(selectorParamsTabNum);
  const isServer = useStore(selectorIsServer);

  const save = useCallback(() => {
    saveServerParams().then(() => {
      setDashboardLayoutEdit(false);
    });
  }, [saveServerParams, setDashboardLayoutEdit]);

  return (
    <div>
      {params.plots.length > 0 && !embed && <DashboardHeader />}
      <ErrorMessages />
      {dashboardLayoutEdit && (
        <ul className="nav nav-tabs mb-4 container-xl">
          <li className="nav-item">
            <PlotLink className={cn('nav-link', tabNum === -1 && 'active')} indexPlot={-1} isLink>
              Layout
            </PlotLink>
          </li>
          <li className="nav-item">
            <PlotLink className={cn('nav-link', tabNum === -2 && 'active')} indexPlot={-2} isLink>
              Setting
            </PlotLink>
          </li>

          <li className="nav-item flex-grow-1"></li>

          <Tooltip<'li'>
            as="li"
            className="nav-item "
            titleClassName="bg-warning-subtle"
            title={!params.dashboard?.name && 'Required name dashboard'}
          >
            <Button
              type="button"
              className="nav-link"
              disabled={numQueries > 0 || !params.dashboard?.name}
              onClick={save}
              title={!params.dashboard?.name ? 'Required name dashboard' : 'Save dashboard'}
            >
              {numQueries > 0 ? (
                <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
              ) : (
                <SVGCloudArrowUp className="mx-1" />
              )}
              {isServer ? 'Save' : 'Create'}
            </Button>
          </Tooltip>
        </ul>
      )}
      {params.variables.length > 0 && tabNum === -1 && (
        <DashboardVariablesControl
          className={cn(
            'd-flex flex-grow-1 flex-row gap-3 flex-wrap col-12 justify-content-start container-xl mb-3 z-100 position-relative'
          )}
          embed={embed}
        />
      )}
      <DashboardLayout
        yAxisSize={yAxisSize}
        className={cn('z-10', params.tabNum === -1 ? 'position-relative' : 'hidden-dashboard')}
        embed={embed}
      />
      {params.tabNum === -2 && <DashboardSettings />}
    </div>
  );
};
