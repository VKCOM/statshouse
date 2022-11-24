// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
  selectorLastError,
  selectorListMetricsGroup,
  selectorLoadListMetricsGroup,
  selectorLoadMetricsGroup,
  selectorRemoveMetricsGroup,
  selectorSaveMetricsGroup,
  selectorSelectMetricsGroup,
  selectorSetLastError,
  selectorSetSelectMetricsGroup,
  useStore,
} from '../../store';
import { ReactComponent as SVGPlus } from 'bootstrap-icons/icons/plus.svg';
import { useStateInput } from '../../hooks';
import cn from 'classnames';
import { sortByKey } from '../utils';

export type GroupPageProps = {};
export const GroupPage: React.FC<GroupPageProps> = () => {
  const loadListMetricsGroup = useStore(selectorLoadListMetricsGroup);
  const loadMetricsGroup = useStore(selectorLoadMetricsGroup);
  const listMetricsGroup = useStore(selectorListMetricsGroup);
  const selectMetricsGroup = useStore(selectorSelectMetricsGroup);
  const saveMetricsGroup = useStore(selectorSaveMetricsGroup);
  const removeMetricsGroup = useStore(selectorRemoveMetricsGroup);
  const setSelectMetricsGroup = useStore(selectorSetSelectMetricsGroup);
  const nameMetricsGroupInput = useStateInput(selectMetricsGroup?.group.name ?? '');
  const weightMetricsGroupInput = useStateInput(selectMetricsGroup?.group.weight.toString() ?? '');
  const globalError = useStore(selectorLastError);
  const setGlobalError = useStore(selectorSetLastError);
  const [saveLoader, setSaveLoader] = useState(false);
  const [loadLoader, setLoadLoader] = useState(false);

  const sumWeight = useMemo(
    () => listMetricsGroup.reduce((res, item) => res + (item.weight ?? 0), 1),
    [listMetricsGroup]
  );

  const filterList = useMemo(() => {
    const result = listMetricsGroup.map((item) => ({
      ...item,
      percent: Math.round(((item.weight ?? 0) / sumWeight) * 1000) / 10 ?? 0,
    }));
    result.sort(sortByKey.bind(null, 'name'));
    return result;
  }, [listMetricsGroup, sumWeight]);

  const onAddNewMetricsGroup = useCallback(() => {
    setSelectMetricsGroup({ group: { name: '', weight: 1 }, metrics: [] });
  }, [setSelectMetricsGroup]);

  const onSaveMetricsGroup = useCallback(
    (event: React.FormEvent) => {
      setSaveLoader(true);
      saveMetricsGroup({
        group_id: selectMetricsGroup?.group.group_id,
        version: selectMetricsGroup?.group.version,
        name: nameMetricsGroupInput.value,
        weight: parseInt(weightMetricsGroupInput.value),
      }).finally(() => {
        setSaveLoader(false);
        setLoadLoader(true);
        loadListMetricsGroup().finally(() => {
          setSelectMetricsGroup(undefined);
          setLoadLoader(false);
        });
      });
      event.preventDefault();
    },
    [
      loadListMetricsGroup,
      nameMetricsGroupInput.value,
      saveMetricsGroup,
      selectMetricsGroup,
      setSelectMetricsGroup,
      weightMetricsGroupInput.value,
    ]
  );
  const onRemoveMetricsGroup = useCallback(() => {
    setSaveLoader(true);
    removeMetricsGroup({
      group_id: selectMetricsGroup?.group.group_id,
      version: selectMetricsGroup?.group.version,
      name: nameMetricsGroupInput.value,
      weight: parseInt(weightMetricsGroupInput.value),
    }).finally(() => {
      setSaveLoader(false);
      setLoadLoader(true);
      loadListMetricsGroup().finally(() => {
        setSelectMetricsGroup(undefined);
        setLoadLoader(false);
      });
    });
  }, [
    loadListMetricsGroup,
    nameMetricsGroupInput.value,
    removeMetricsGroup,
    selectMetricsGroup?.group.group_id,
    selectMetricsGroup?.group.version,
    setSelectMetricsGroup,
    weightMetricsGroupInput.value,
  ]);

  const onCancelMetricsGroup = useCallback(() => {
    setSelectMetricsGroup(undefined);
  }, [setSelectMetricsGroup]);

  const onSelectMetricsGroup = useCallback(
    (event: React.MouseEvent) => {
      const id = parseInt(event.currentTarget.getAttribute('data-id') ?? '-1');
      setLoadLoader(true);
      loadMetricsGroup(id).finally(() => {
        setLoadLoader(false);
      });
    },
    [loadMetricsGroup]
  );

  const defaultMetricsGroupWeightPercent = useMemo(() => Math.round((1 / sumWeight) * 1000) / 10, [sumWeight]);

  const selectMetricsGroupWeightPercent = useMemo(() => {
    const weight = parseFloat(weightMetricsGroupInput.value.replace(',', '.')) ?? 0;
    if (weight === 0) {
      return 0;
    }
    return (
      Math.round(
        (weight /
          (sumWeight -
            ((typeof selectMetricsGroup?.group.group_id !== 'undefined' && selectMetricsGroup?.group.weight) || 0) +
            weight)) *
          1000
      ) / 10
    );
  }, [selectMetricsGroup?.group.group_id, selectMetricsGroup?.group.weight, sumWeight, weightMetricsGroupInput.value]);

  const errorClear = useCallback(() => {
    setGlobalError('');
  }, [setGlobalError]);

  useEffect(() => {
    setLoadLoader(true);
    loadListMetricsGroup().finally(() => {
      setLoadLoader(false);
    });
    return () => setLoadLoader(false);
  }, [loadListMetricsGroup]);

  return (
    <div className="flex-grow-1 p-2">
      {!!globalError && (
        <div className="alert alert-danger d-flex align-items-center justify-content-between pb-2">
          <small className="overflow-force-wrap font-monospace">{globalError}</small>
          <button type="button" className="btn-close" aria-label="Close" onClick={errorClear}></button>
        </div>
      )}
      <div className="row">
        <div className={cn('col-md-6 w-max-720', !!selectMetricsGroup && 'hidden-down-md')}>
          <div className="mb-2 d-flex flex-row justify-content-end">
            <button
              className="btn btn-outline-primary ms-2 text-nowrap"
              title="Add group"
              onClick={onAddNewMetricsGroup}
            >
              <SVGPlus />
              Add group
            </button>
          </div>
          <ul className="list-group">
            <li className="list-group-item text-secondary d-flex flex-row">
              <div className="flex-grow-1">default</div>
              <div>1 [{defaultMetricsGroupWeightPercent}%]</div>
            </li>
            {filterList.map((item) => (
              <li
                key={item.id}
                data-id={item.id}
                role="button"
                className={cn(
                  'list-group-item text-black d-flex flex-row',
                  selectMetricsGroup?.group.group_id === item.id && 'text-bg-light'
                )}
                onClick={onSelectMetricsGroup}
              >
                <div className="flex-grow-1">{item.name}</div>
                <div>
                  {item.weight} [{item.percent}%]
                </div>
              </li>
            ))}
            {loadLoader && (
              <div className="text-center mt-2">
                <span
                  className="spinner-border spinner-border-sm me-2 text-primary"
                  role="status"
                  aria-hidden="true"
                ></span>
              </div>
            )}
          </ul>
        </div>
        {!!selectMetricsGroup && (
          <div className="col-md-6 w-max-720">
            <form onSubmit={onSaveMetricsGroup}>
              <div className="mb-3 row">
                <label htmlFor="metricsGroupName" className="col-sm-2 col-form-label">
                  Name
                </label>
                <div className="col-sm-10">
                  <input type="text" className="form-control" id="metricsGroupName" {...nameMetricsGroupInput} />
                </div>
              </div>
              <div className="mb-3 row">
                <label htmlFor="metricsGroupWeight" className="col-sm-2 col-form-label">
                  Weight
                </label>
                <div className="col-sm-10 d-flex flex-row">
                  <input
                    type="number"
                    min={0}
                    max={1000}
                    step={1}
                    className="form-control"
                    id="metricsGroupWeight"
                    {...weightMetricsGroupInput}
                  />
                  <div className="col-form-label ms-2">[{selectMetricsGroupWeightPercent}%]</div>
                </div>
              </div>
              <div className="mb-3 row">
                <span className="col-sm-2 col-form-label">Metrics</span>
                <div className="col-sm-10 d-flex flex-row flex-wrap align-items-start">
                  {selectMetricsGroup.metrics?.map((metric_name, index) => (
                    <span
                      className="input-group-text border-success bg-transparent text-success text-nowrap py-0 mt-1 me-1 "
                      key={index}
                    >
                      <span className="small">{metric_name}</span>
                    </span>
                  ))}
                </div>
              </div>
              <div className="mb-3 d-flex flex-row justify-content-end">
                <button
                  type="submit"
                  className="btn btn-outline-primary ms-1 text-nowrap"
                  disabled={!nameMetricsGroupInput.value || saveLoader}
                >
                  {saveLoader && (
                    <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                  )}
                  {typeof selectMetricsGroup.group.group_id === 'undefined' ? 'Create' : 'Save'}
                </button>
                <button
                  type="button"
                  className="btn btn-outline-primary ms-1 text-nowrap"
                  onClick={onCancelMetricsGroup}
                >
                  Cancel
                </button>
                {typeof selectMetricsGroup.group.group_id !== 'undefined' && (
                  <button
                    type="button"
                    className="btn btn-outline-primary ms-1 text-nowrap"
                    onClick={onRemoveMetricsGroup}
                    disabled={!nameMetricsGroupInput.value || saveLoader}
                  >
                    {saveLoader && (
                      <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                    )}
                    Remove
                  </button>
                )}
              </div>
            </form>
          </div>
        )}
      </div>
    </div>
  );
};
