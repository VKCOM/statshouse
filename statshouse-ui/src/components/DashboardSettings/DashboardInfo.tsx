// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useState } from 'react';
import {
  selectorIsServer,
  selectorParams,
  selectorRemoveServerParams,
  selectorSaveServerParams,
  selectorSetParams,
  useStore,
} from '../../store';
import { useStateInput } from '../../hooks';
import produce from 'immer';
import { useNavigate } from 'react-router-dom';

export type DashboardInfoProps = {};

export const DashboardInfo: React.FC<DashboardInfoProps> = () => {
  const params = useStore(selectorParams);
  const setParams = useStore(selectorSetParams);
  const saveServerParams = useStore(selectorSaveServerParams);
  const removeServerParams = useStore(selectorRemoveServerParams);
  const isServer = useStore(selectorIsServer);
  const nameInput = useStateInput(params.dashboard?.name ?? '');
  const descriptionInput = useStateInput(params.dashboard?.description ?? '');
  const [saveSpinner, setSaveSpinner] = useState(false);
  const [saveError, setSaveError] = useState('');
  const navigate = useNavigate();

  const onSubmit = useCallback<React.FormEventHandler<HTMLFormElement>>(
    (event) => {
      const dashboard = {
        ...params.dashboard,
        name: nameInput.value,
        description: descriptionInput.value,
      };
      setParams(
        produce((params) => {
          params.dashboard = dashboard;
        })
      );
      setSaveSpinner(true);
      setSaveError('');
      saveServerParams()
        .catch((error: string) => {
          setSaveError(error);
        })
        .finally(() => {
          setSaveSpinner(false);
        });
      event.preventDefault();
    },
    [descriptionInput.value, nameInput.value, params.dashboard, saveServerParams, setParams]
  );

  const onRemoveDashboard = useCallback(
    (event: React.MouseEvent) => {
      if (params.dashboard?.dashboard_id && window.confirm(`Remove dashboard ${params.dashboard?.name}?`)) {
        setSaveSpinner(true);
        setSaveError('');
        removeServerParams()
          .catch((error: string) => {
            setSaveError(error);
          })
          .then(() => {
            setParams(
              produce((params) => {
                params.dashboard = undefined;
              })
            );
            navigate('/dash-list');
          })
          .finally(() => {
            setSaveSpinner(false);
          });
      }
      event.preventDefault();
    },
    [navigate, params.dashboard?.dashboard_id, params.dashboard?.name, removeServerParams, setParams]
  );

  const errorClear = useCallback(() => {
    setSaveError('');
  }, []);

  return (
    <div className="card border-0">
      <div className="card-body p-2">
        <h5 className="card-title">Dashboard Info</h5>
        {!!saveError && (
          <div className="alert alert-danger d-flex align-items-center justify-content-between">
            <small className="overflow-force-wrap font-monospace">{saveError}</small>
            <button type="button" className="btn-close" aria-label="Close" onClick={errorClear}></button>
          </div>
        )}
        <form onSubmit={onSubmit} className="card-text">
          <div className="mb-2 row">
            <label htmlFor="dashboard-input-name" className="col-form-label col-sm-2">
              Name
            </label>
            <div className="col-sm-10">
              <input id="dashboard-input-name" type="text" className="form-control" aria-label="Name" {...nameInput} />
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
                {...descriptionInput}
              />
            </div>
          </div>
          <div className="d-flex flex-row justify-content-end">
            <button type="submit" className="btn btn-outline-primary" disabled={saveSpinner}>
              {saveSpinner && (
                <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
              )}
              {isServer ? 'Save' : 'Create'}
            </button>
            {params.dashboard?.dashboard_id !== undefined && (
              <button type="button" className="btn btn-outline-danger ms-2" onClick={onRemoveDashboard}>
                Remove
              </button>
            )}
          </div>
        </form>
      </div>
    </div>
  );
};
