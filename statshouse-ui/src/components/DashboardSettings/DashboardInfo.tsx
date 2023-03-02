// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback } from 'react';
import {
  selectorGlobalNumQueriesPlot,
  selectorIsServer,
  selectorParams,
  selectorRemoveServerParams,
  selectorSetDashboardLayoutEdit,
  selectorSetParams,
  useStore,
} from '../../store';
import produce from 'immer';
import { useNavigate } from 'react-router-dom';

export type DashboardInfoProps = {};

export const DashboardInfo: React.FC<DashboardInfoProps> = () => {
  const params = useStore(selectorParams);
  const setParams = useStore(selectorSetParams);
  const removeServerParams = useStore(selectorRemoveServerParams);
  const isServer = useStore(selectorIsServer);
  const numQueries = useStore(selectorGlobalNumQueriesPlot);
  const setDashboardLayoutEdit = useStore(selectorSetDashboardLayoutEdit);

  const navigate = useNavigate();

  const inputName = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const value = e.target.value;
      setParams(
        produce((params) => {
          params.dashboard = params.dashboard ?? {};
          params.dashboard.name = value;
        })
      );
    },
    [setParams]
  );
  const inputDescription = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const value = e.target.value;
      setParams(
        produce((params) => {
          params.dashboard = params.dashboard ?? {};
          params.dashboard.description = value;
        })
      );
    },
    [setParams]
  );

  const onRemoveDashboard = useCallback(
    (event: React.MouseEvent) => {
      if (params.dashboard?.dashboard_id && window.confirm(`Remove dashboard ${params.dashboard?.name}?`)) {
        removeServerParams().then(() => {
          setDashboardLayoutEdit(false);
          setParams(
            produce((params) => {
              params.dashboard = undefined;
            })
          );
          navigate('/dash-list');
        });
      }
      event.preventDefault();
    },
    [
      navigate,
      params.dashboard?.dashboard_id,
      params.dashboard?.name,
      removeServerParams,
      setDashboardLayoutEdit,
      setParams,
    ]
  );

  return (
    <div className="card border-0">
      <div className="card-body p-2">
        <h5 className="card-title">Dashboard Info</h5>
        <div className="card-text">
          <div className="mb-2 row">
            <label htmlFor="dashboard-input-name" className="col-form-label col-sm-2">
              Name
            </label>
            <div className="col-sm-10">
              <input
                id="dashboard-input-name"
                type="text"
                className="form-control"
                aria-label="Name"
                defaultValue={params.dashboard?.name ?? ''}
                onInput={inputName}
              />
            </div>
          </div>
          <div className="mb-2 row">
            <label htmlFor="dashboard-input-description" className="col-form-label col-sm-2">
              Description
            </label>
            <div className="col-sm-10">
              <input
                id="dashboard-input-description"
                type="text"
                className="form-control"
                aria-label="Name"
                defaultValue={params.dashboard?.description ?? ''}
                onInput={inputDescription}
              />
            </div>
          </div>
          <div className="d-flex flex-row justify-content-end">
            {isServer && (
              <button
                type="button"
                className="btn btn-outline-danger ms-2"
                onClick={onRemoveDashboard}
                disabled={numQueries > 1}
              >
                Remove
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};
