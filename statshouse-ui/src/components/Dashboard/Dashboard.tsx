// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useMemo } from 'react';
import {
  selectorDashboardLayoutEdit,
  selectorGlobalNumQueriesPlot,
  selectorIsServer,
  selectorLastError,
  selectorParams,
  selectorParamsTabNum,
  selectorParamsTagSync,
  selectorSaveServerParams,
  selectorSetDashboardLayoutEdit,
  selectorSetLastError,
  useStore,
} from '../../store';
import { DashboardHeader } from './DashboardHeader';
import { DashboardLayout } from './DashboardLayout';
import { DashboardSettings } from '../DashboardSettings';
import { DashboardTagControl } from '../DashboardTagControl';
import cn from 'classnames';
import css from './style.module.css';
import { PlotLink } from '../Plot/PlotLink';

export type DashboardProps = {
  yAxisSize?: number;
  embed?: boolean;
};

export const Dashboard: React.FC<DashboardProps> = ({ embed = false, yAxisSize = 54 }) => {
  const params = useStore(selectorParams);
  const numQueries = useStore(selectorGlobalNumQueriesPlot);
  const lastError = useStore(selectorLastError);
  const setLastError = useStore(selectorSetLastError);
  const tagsSync = useStore(selectorParamsTagSync);
  const showSyncPanel = useMemo(() => tagsSync.some((group) => group.some((s) => s !== null)), [tagsSync]);

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

  const onCloseLayoutEdit = useCallback(() => {
    setDashboardLayoutEdit(false);
  }, [setDashboardLayoutEdit]);
  const lastErrorClear = useCallback(() => {
    setLastError('');
  }, [setLastError]);

  return (
    <div>
      {params.plots.length > 0 && !embed && <DashboardHeader />}
      {lastError && (
        <div className="alert alert-danger d-flex align-items-center justify-content-between">
          <small className="overflow-force-wrap font-monospace">{lastError}</small>
          <button type="button" className="btn-close" aria-label="Close" onClick={lastErrorClear}></button>
        </div>
      )}
      {dashboardLayoutEdit && (
        <ul className="nav nav-tabs mb-4">
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

          <li className="nav-item">
            <button
              type="button"
              className="nav-link"
              disabled={numQueries > 0 || !params.dashboard?.name}
              onClick={save}
              title={!params.dashboard?.name ? 'Required name dashboard' : 'Save dashboard'}
            >
              {numQueries > 0 && (
                <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
              )}
              {isServer ? 'Save' : 'Create'}
            </button>
          </li>

          <li className="nav-item">
            <button type="button" className="nav-link" onClick={onCloseLayoutEdit}>
              Close
            </button>
          </li>
        </ul>
      )}
      {showSyncPanel && tabNum === -1 && (
        <DashboardTagControl
          className={cn('d-flex flex-grow-1 flex-row gap-3 flex-wrap col-12 justify-content-start', css.margin)}
        />
      )}
      <DashboardLayout yAxisSize={yAxisSize} className={params.tabNum === -1 ? '' : 'hidden-dashboard'} />
      {params.tabNum === -2 && <DashboardSettings />}
    </div>
  );
};
