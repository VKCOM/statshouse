// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useState } from 'react';
import { selectorLoadPromConfig, selectorPromConfig, selectorSavePromConfig, useStore } from '../../store';
import { useStateInput } from '../../hooks';
import { ErrorMessages } from 'components/ErrorMessages';

export type PrometheusPageProps = {};
export const PrometheusPage: React.FC<PrometheusPageProps> = () => {
  const promConfig = useStore(selectorPromConfig);
  const loadPromConfig = useStore(selectorLoadPromConfig);
  const savePromConfig = useStore(selectorSavePromConfig);

  const configInput = useStateInput(promConfig?.config ?? '');

  const [loader, setLoader] = useState(false);

  useEffect(() => {
    setLoader(true);
    loadPromConfig().finally(() => {
      setLoader(false);
    });
    return () => setLoader(false);
  }, [loadPromConfig]);

  const onSubmit = useCallback(
    (event: React.FormEvent) => {
      event.preventDefault();
      setLoader(true);
      savePromConfig({ config: configInput.value, version: promConfig?.version ?? 0 }).finally(() => {
        setLoader(false);
      });
    },
    [configInput.value, promConfig?.version, savePromConfig]
  );

  return (
    <div className="flex-grow-1 p-2">
      <ErrorMessages />
      <form onSubmit={onSubmit}>
        <div className="mb-2">
          <textarea className="form-control" rows={20} {...configInput}></textarea>
        </div>
        <div className="mb-3 d-flex flex-row justify-content-end">
          <button type="submit" className="btn btn-outline-primary ms-1 text-nowrap" disabled={loader}>
            {loader && <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>}
            Save
          </button>
        </div>
      </form>
    </div>
  );
};
