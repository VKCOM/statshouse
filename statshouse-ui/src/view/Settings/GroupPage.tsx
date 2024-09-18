// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { groupAdd, groupListErrors, groupListLoad, groupLoad, groupSave, useGroupListStore } from 'store/group';
import { ReactComponent as SVGPlus } from 'bootstrap-icons/icons/plus.svg';
import cn from 'classnames';
import { GroupInfo, GroupMetric } from 'api/group';
import { toNumber } from 'common/helpers';
import { produce } from 'immer';
import { ErrorMessages } from 'components/ErrorMessages';
import { InputText } from 'components/UI';

type SelectGroup = {
  group: Pick<GroupInfo, 'name' | 'weight' | 'namespace_id'> &
    Partial<Omit<GroupInfo, 'name' | 'weight' | 'namespace_id'>>;
  metrics: GroupMetric[] | null;
};

export function GroupPage() {
  const listMetricsGroup = useGroupListStore((s) => s.list);
  const [selectMetricsGroup, setSelectMetricsGroup] = useState<SelectGroup | null>(null);
  const [saveLoader, setSaveLoader] = useState(false);
  const [loadLoader, setLoadLoader] = useState(false);

  const selectId = selectMetricsGroup?.group.group_id ?? null;
  const selectWeight = selectMetricsGroup?.group.weight ?? 0;
  const selectDisable = selectMetricsGroup?.group.disable ?? false;

  const { sumWeight, list } = useMemo(() => {
    let sumWeight = selectId == null ? selectWeight : 0;
    listMetricsGroup.forEach((g) => {
      if (!g.disable) {
        const weight = selectId === g.id ? selectWeight : g.weight;
        sumWeight += weight;
      }
    });

    const list = listMetricsGroup.map((g) => {
      if (g.disable) {
        return { ...g, weight: 0, percent: 0 };
      }
      const weight = selectId === g.id ? selectWeight : g.weight;
      const percent = Math.round((weight / sumWeight) * 1000) / 10;
      return { ...g, weight, percent };
    });
    return { sumWeight, list };
  }, [listMetricsGroup, selectId, selectWeight]);

  const onAddNewMetricsGroup = useCallback(() => {
    setSelectMetricsGroup({ group: { name: '', weight: 1, namespace_id: 0 }, metrics: [] });
  }, []);

  const onSaveMetricsGroup = useCallback(
    (event: React.FormEvent) => {
      if (selectMetricsGroup) {
        setSaveLoader(true);
        if (selectMetricsGroup.group.group_id != null && selectMetricsGroup.group.version != null) {
          groupSave({
            group: {
              group_id: -1,
              version: 0,
              ...selectMetricsGroup.group,
            },
          })
            .then((g) => {
              if (g) {
                const { metrics, group } = g;
                metrics?.sort();
                setSelectMetricsGroup({ group, metrics });
              }
            })
            .finally(() => {
              setSaveLoader(false);
            });
        } else {
          groupAdd({
            group: { name: selectMetricsGroup.group.name, weight: selectMetricsGroup.group.weight },
          })
            .then((g) => {
              if (g) {
                const { metrics, group } = g;
                metrics?.sort();
                setSelectMetricsGroup({ group, metrics });
              }
            })
            .finally(() => {
              setSaveLoader(false);
            });
        }
      } else {
        setSaveLoader(false);
      }
      event.preventDefault();
    },
    [selectMetricsGroup]
  );
  const onRemoveMetricsGroup = useCallback(() => {
    if (selectMetricsGroup) {
      if (!window.confirm('Confirm ' + (selectMetricsGroup.group.disable ? 'restore' : 'remove'))) {
        return;
      }
      setSaveLoader(true);
      if (selectMetricsGroup.group.group_id != null && selectMetricsGroup.group.version != null) {
        groupSave({
          group: {
            group_id: -1,
            version: 0,
            ...selectMetricsGroup.group,
            disable: !selectMetricsGroup.group.disable,
          },
        })
          .then((g) => {
            if (g) {
              const { metrics, group } = g;
              metrics?.sort();
              setSelectMetricsGroup({ group, metrics });
            }
          })
          .finally(() => {
            setSaveLoader(false);
          });
      }
    }
  }, [selectMetricsGroup]);

  const onCancelMetricsGroup = useCallback(() => {
    setSelectMetricsGroup(null);
  }, [setSelectMetricsGroup]);

  const onSelectMetricsGroup = useCallback((event: React.MouseEvent) => {
    const id = parseInt(event.currentTarget.getAttribute('data-id') ?? '-1');

    setLoadLoader(true);
    groupLoad(id)
      .then((g) => {
        if (g) {
          const { metrics, group } = g;
          metrics?.sort();
          setSelectMetricsGroup({ group, metrics });
        } else {
          setSelectMetricsGroup(null);
        }
      })
      .finally(() => {
        setLoadLoader(false);
      });
  }, []);

  const selectMetricsGroupWeightPercent = useMemo(
    () => Math.round((selectWeight / (sumWeight + (selectDisable ? selectWeight : 0))) * 1000) / 10,
    [selectDisable, selectWeight, sumWeight]
  );

  useEffect(() => {
    setLoadLoader(true);
    groupListLoad().finally(() => {
      setLoadLoader(false);
    });
    return () => setLoadLoader(false);
  }, []);

  const onChangeName = useCallback((value: string) => {
    setSelectMetricsGroup(
      produce((g) => {
        if (g) {
          g.group.name = value;
        }
      })
    );
  }, []);
  const onChangeWeight = useCallback((value: string) => {
    setSelectMetricsGroup(
      produce((g) => {
        if (g) {
          g.group.weight = toNumber(value, 1);
        }
      })
    );
  }, []);

  return (
    <div className="flex-grow-1 p-2">
      <ErrorMessages channel={groupListErrors} />
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
            {list.map((item) => (
              <li
                key={item.id}
                data-id={item.id}
                role="button"
                className={cn(
                  item.disable ? 'text-secondary' : 'text-body',
                  'list-group-item d-flex flex-row',
                  selectMetricsGroup?.group.group_id === item.id && 'text-bg-light'
                )}
                onClick={onSelectMetricsGroup}
              >
                <div className={cn('flex-grow-1', item.disable && 'text-decoration-line-through text-secondary')}>
                  {item.name}
                </div>
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
                  <InputText
                    type="text"
                    className="form-control"
                    id="metricsGroupName"
                    disabled={selectMetricsGroup.group.group_id != null && selectMetricsGroup.group.group_id <= 0}
                    value={selectMetricsGroup.group.name}
                    onChange={onChangeName}
                  />
                </div>
              </div>
              <div className="mb-3 row">
                <label htmlFor="metricsGroupWeight" className="col-sm-2 col-form-label">
                  Weight
                </label>
                <div className="col-sm-10 d-flex flex-row">
                  <InputText
                    type="number"
                    min={0}
                    max={1000}
                    step={0.01}
                    className="form-control"
                    id="metricsGroupWeight"
                    value={selectMetricsGroup.group.weight.toString()}
                    onChange={onChangeWeight}
                  />
                  <div className="col-form-label ms-2" style={{ width: 80 }}>
                    [{selectMetricsGroupWeightPercent}%]
                  </div>
                </div>
              </div>
              <div className="mb-3 row">
                <span className="col-sm-2 col-form-label">Metrics</span>
                <div
                  className="col-sm-10 d-flex flex-row flex-wrap align-items-start overflow-auto"
                  style={{ maxHeight: 300 }}
                >
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
                  disabled={!selectMetricsGroup.group.name || saveLoader}
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
                {selectMetricsGroup.group.group_id != null && selectMetricsGroup.group.group_id > 0 && (
                  <button
                    type="button"
                    className="btn btn-outline-danger ms-1 text-nowrap"
                    onClick={onRemoveMetricsGroup}
                    disabled={!selectMetricsGroup.group.name || saveLoader}
                  >
                    {saveLoader && (
                      <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                    )}
                    {selectMetricsGroup.group.disable ? 'Restore' : 'Remove'}
                  </button>
                )}
              </div>
            </form>
          </div>
        )}
      </div>
    </div>
  );
}

export default GroupPage;
