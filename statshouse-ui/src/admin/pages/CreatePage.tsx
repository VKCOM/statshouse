// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import * as React from 'react';
import { useNavigate } from 'react-router-dom';
import { IMetric } from '../models/metric';
import { saveMetric } from '../api/saveMetric';
import { maxTagsSize } from '../../common/settings';
import { getDefaultTag } from '../storages/MetricFormValues/reducer';

export function CreatePage(props: { yAxisSize: number }) {
  const { yAxisSize } = props;

  React.useEffect(() => {
    document.title = 'New metric — StatsHouse';
  }, []);

  return (
    <div className="container-xl pt-3 pb-3">
      <div style={{ paddingLeft: `${yAxisSize}px` }}>
        <h6 className="overflow-force-wrap font-monospace fw-bold me-3 mb-3">New metric</h6>
        <EditFormCreate />
      </div>
    </div>
  );
}

export function EditFormCreate() {
  const [name, setName] = React.useState('');
  const { onSubmit, isRunning, error, success } = useSubmitCreate(name);

  return (
    <form>
      <div className="col-sm-5 mb-3 form-text">
        Metric will be created with all {maxTagsSize} keys visible. To hide excess keys, please use <b>Edit</b> button
        above plot.
      </div>
      <div className="row mb-3">
        <label htmlFor="name" className="col-sm-2 col-form-label">
          Name
        </label>
        <div className="col-sm-5">
          <input
            id="name"
            name="name"
            type="text"
            className="form-control"
            value={name}
            onChange={(e) => setName(e.target.value)}
          />
          <div className="form-text">Use only Latin letters, integer or underscores.</div>
        </div>
      </div>

      <div>
        <button type="button" disabled={isRunning} className="btn btn-primary me-3" onClick={onSubmit}>
          {'Create'}
        </button>
        {isRunning ? (
          <div className="spinner-border spinner-border-sm" role="status">
            <span className="visually-hidden">Loading...</span>
          </div>
        ) : error ? (
          <span className="text-danger">{error}</span>
        ) : success ? (
          <span className="text-success">{success}</span>
        ) : null}
      </div>
    </form>
  );
}

function useSubmitCreate(name: string) {
  const navigate = useNavigate();
  const [isRunning, setRunning] = React.useState<boolean>(false);
  const [error, setError] = React.useState<string | null>(null);
  const [success, setSuccess] = React.useState<string | null>(null);

  const onSubmit = React.useCallback(() => {
    setError(null);
    setSuccess(null);
    setRunning(true);

    const values: IMetric = {
      id: 0,
      name: name,
      description: '',
      kind: 'mixed',
      stringTopName: '',
      stringTopDescription: '',
      weight: 1,
      resolution: 1,
      withPercentiles: false,
      visible: true,
      disable: false,
      tags: [
        { name: '', alias: 'environment', customMapping: [] }, // env
        ...new Array(maxTagsSize - 1).fill({}).map(() => getDefaultTag()),
      ],
      tags_draft: [],
      tagsSize: maxTagsSize,
    };
    saveMetric(values)
      .then(() => {
        setSuccess('Saved');
        setRunning(false);
        navigate(`/view?s=${name}`, { replace: true });
      })
      .catch((err) => {
        setError(err.message);
        setRunning(false);
      });
  }, [name, navigate]);

  return {
    isRunning,
    onSubmit,
    error,
    success,
  };
}
