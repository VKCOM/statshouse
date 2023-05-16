// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useState } from 'react';
import {
  selectorMetricsMeta,
  selectorParamsPlots,
  selectorParamsTagSync,
  selectorPlotsData,
  useStore,
} from '../../store';
import { SyncTagGroup } from './SyncTagGroup';
import { ReactComponent as SVGX } from 'bootstrap-icons/icons/x.svg';
import { ReactComponent as SVGCheckLg } from 'bootstrap-icons/icons/check-lg.svg';
import { ReactComponent as SVGPencil } from 'bootstrap-icons/icons/pencil.svg';
import produce from 'immer';
import { notNull } from '../../view/utils';

const loadMetricsMeta = useStore.getState().loadMetricsMeta;
const setParams = useStore.getState().setParams;
const preSync = useStore.getState().preSync;

export type DashboardTagSyncProp = {};
export const DashboardTagSync: React.FC<DashboardTagSyncProp> = () => {
  const tagsSync = useStore(selectorParamsTagSync);
  const plots = useStore(selectorParamsPlots);
  const metricsMeta = useStore(selectorMetricsMeta);
  const plotsData = useStore(selectorPlotsData);
  const [valueSync, setValueSync] = useState(tagsSync);
  const [edit, setEdit] = useState(false);

  useEffect(() => {
    setValueSync(tagsSync);
  }, [tagsSync]);

  const onSetTagSync = useCallback((indexGroup: number, indexPlot: number, indexTag: number, status: boolean) => {
    if (indexGroup >= 0 && indexPlot >= 0 && indexTag >= 0) {
      setValueSync(
        produce((tagSync) => {
          tagSync[indexGroup][indexPlot] = status ? indexTag : null;
        })
      );
    } else if (indexGroup === -1 && indexPlot === -1 && indexTag === -1 && status) {
      setValueSync(
        produce((tagSync) => {
          tagSync.push([]);
        })
      );
    } else if (indexGroup >= 0 && indexPlot === -1 && indexTag === -1 && !status) {
      setValueSync(
        produce((tagSync) => {
          tagSync.splice(indexGroup, 1);
        })
      );
    }
  }, []);

  const addGroup = useCallback(() => {
    onSetTagSync(-1, -1, -1, true);
  }, [onSetTagSync]);

  const onEdit = useCallback(() => {
    setValueSync(tagsSync);
    if (!tagsSync.filter((g) => g.filter(notNull).length > 0).length) {
      onSetTagSync(-1, -1, -1, true);
    }
    setEdit(true);
  }, [onSetTagSync, tagsSync]);

  const onApply = useCallback(() => {
    setParams(
      produce((params) => {
        params.tagSync = valueSync.filter((g) => g.filter(notNull).length > 0);
      })
    );
    setEdit(false);
    setValueSync(tagsSync);
    preSync();
  }, [tagsSync, valueSync]);

  const onCancel = useCallback(() => {
    setValueSync(tagsSync);
    setEdit(false);
  }, [tagsSync]);

  useEffect(() => {
    plots.forEach((p) => {
      loadMetricsMeta(p.metricName);
    });
  }, [plots]);

  return (
    <div className="card border-0">
      <div className="card-body p-2">
        <h5 className="card-title">Sync Tags</h5>
        <div className="card-text d-flex flex-column flex-wrap gap-1">
          {valueSync.map((group, indexGroup) => (
            <SyncTagGroup
              key={indexGroup}
              indexGroup={indexGroup}
              syncTags={group}
              setTagSync={onSetTagSync}
              plots={plots}
              plotsData={plotsData}
              metricsMeta={metricsMeta}
              edit={edit}
            />
          ))}
        </div>
        <div className="mt-1 text-end">
          {edit ? (
            <>
              <button type="button" onClick={addGroup} className="btn btn-outline-primary">
                Add group
              </button>

              <button type="button" className="btn btn-outline-success ms-2" onClick={onApply}>
                <SVGCheckLg /> Apply
              </button>

              <button type="button" className="btn btn-outline-danger ms-2" onClick={onCancel}>
                <SVGX /> Cancel
              </button>
            </>
          ) : (
            <>
              <button type="button" className="btn btn-outline-primary" onClick={onEdit}>
                <SVGPencil /> Edit
              </button>
            </>
          )}
        </div>
      </div>
    </div>
  );
};
