// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
  namespaceAdd,
  namespaceListErrors,
  namespaceListLoad,
  namespaceLoad,
  namespaceSave,
  useNamespaceListStore,
} from '@/store2/namespace';
import { ReactComponent as SVGPlus } from 'bootstrap-icons/icons/plus.svg';
import cn from 'classnames';
import { toNumber } from '@/common/helpers';
import { produce } from 'immer';
import type { Namespace } from '@/api/namespace';
import { ErrorMessages } from '@/components/ErrorMessages';
import { InputText } from '@/components/UI';

type SelectNamespace = {
  namespace: Pick<Namespace, 'name' | 'weight'> & Partial<Omit<Namespace, 'name' | 'weight'>>;
};

export function NamespacePage() {
  const listMetricsNamespace = useNamespaceListStore((s) => s.list);
  const [selectMetricsNamespace, setSelectMetricsNamespace] = useState<SelectNamespace | null>(null);
  const [saveLoader, setSaveLoader] = useState(false);
  const [loadLoader, setLoadLoader] = useState(false);

  const selectId = selectMetricsNamespace?.namespace.namespace_id ?? null;
  const selectWeight = selectMetricsNamespace?.namespace.weight ?? 0;
  const selectDisable = selectMetricsNamespace?.namespace.disable ?? false;

  const { sumWeight, list } = useMemo(() => {
    let sumWeight = selectId == null ? selectWeight : 0;
    listMetricsNamespace.forEach((g) => {
      if (!g.disable) {
        const weight = selectId === g.id ? selectWeight : g.weight;
        sumWeight += weight;
      }
    });
    const list = listMetricsNamespace.map((g) => {
      if (g.disable) {
        return { ...g, weight: 0, percent: 0 };
      }
      const weight = selectId === g.id ? selectWeight : g.weight;
      const percent = Math.round((weight / sumWeight) * 1000) / 10;
      return { ...g, weight, percent };
    });
    return { sumWeight, list };
  }, [listMetricsNamespace, selectId, selectWeight]);

  const onAddNewMetricsNamespace = useCallback(() => {
    setSelectMetricsNamespace({ namespace: { name: '', weight: 1 } });
  }, []);

  const onSaveMetricsNamespace = useCallback(
    (event: React.FormEvent) => {
      if (selectMetricsNamespace) {
        setSaveLoader(true);
        if (selectMetricsNamespace.namespace.namespace_id != null && selectMetricsNamespace.namespace.version != null) {
          namespaceSave({
            namespace: {
              namespace_id: -1,
              version: 0,
              ...selectMetricsNamespace.namespace,
            },
          })
            .then((g) => {
              if (g) {
                const { namespace } = g;
                setSelectMetricsNamespace({ namespace });
              }
            })
            .finally(() => {
              setSaveLoader(false);
            });
        } else {
          namespaceAdd({
            namespace: { name: selectMetricsNamespace.namespace.name, weight: selectMetricsNamespace.namespace.weight },
          })
            .then((g) => {
              if (g) {
                const { namespace } = g;
                setSelectMetricsNamespace({ namespace });
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
    [selectMetricsNamespace]
  );
  const onRemoveMetricsNamespace = useCallback(() => {
    if (selectMetricsNamespace) {
      if (!window.confirm('Confirm ' + (selectMetricsNamespace.namespace.disable ? 'restore' : 'remove'))) {
        return;
      }
      setSaveLoader(true);
      if (selectMetricsNamespace.namespace.namespace_id != null && selectMetricsNamespace.namespace.version != null) {
        namespaceSave({
          namespace: {
            namespace_id: -1,
            version: 0,
            ...selectMetricsNamespace.namespace,
            disable: !selectMetricsNamespace.namespace.disable,
          },
        })
          .then((g) => {
            if (g) {
              const { namespace } = g;
              setSelectMetricsNamespace({ namespace });
            }
          })
          .finally(() => {
            setSaveLoader(false);
          });
      }
    }
  }, [selectMetricsNamespace]);

  const onCancelMetricsNamespace = useCallback(() => {
    setSelectMetricsNamespace(null);
  }, []);

  const onSelectMetricsGroup = useCallback((event: React.MouseEvent) => {
    const id = parseInt(event.currentTarget.getAttribute('data-id') ?? '-1');

    setLoadLoader(true);
    namespaceLoad(id)
      .then((g) => {
        if (g) {
          const { namespace } = g;
          setSelectMetricsNamespace({ namespace });
        } else {
          setSelectMetricsNamespace(null);
        }
      })
      .finally(() => {
        setLoadLoader(false);
      });
  }, []);

  const selectMetricsNamespaceWeightPercent = useMemo(
    () => Math.round((selectWeight / (sumWeight + (selectDisable ? selectWeight : 0))) * 1000) / 10,
    [selectDisable, selectWeight, sumWeight]
  );

  useEffect(() => {
    setLoadLoader(true);
    namespaceListLoad().finally(() => {
      setLoadLoader(false);
    });
    return () => setLoadLoader(false);
  }, []);

  const onChangeName = useCallback((value: string) => {
    setSelectMetricsNamespace(
      produce((g) => {
        if (g) {
          g.namespace.name = value;
        }
      })
    );
  }, []);
  const onChangeWeight = useCallback((value: string) => {
    setSelectMetricsNamespace(
      produce((g) => {
        const v = toNumber(value);
        if (g && v != null) {
          g.namespace.weight = v;
        }
      })
    );
  }, []);

  return (
    <div className="flex-grow-1 p-2">
      <ErrorMessages channel={namespaceListErrors} />
      <div className="row">
        <div className={cn('col-md-6 w-max-720', !!selectMetricsNamespace && 'hidden-down-md')}>
          <div className="mb-2 d-flex flex-row justify-content-end">
            <button
              className="btn btn-outline-primary ms-2 text-nowrap"
              title="Add group"
              onClick={onAddNewMetricsNamespace}
            >
              <SVGPlus />
              Add namespace
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
                  selectMetricsNamespace?.namespace.namespace_id === item.id && 'text-bg-light'
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
        {!!selectMetricsNamespace && (
          <div className="col-md-6 w-max-720">
            <form onSubmit={onSaveMetricsNamespace}>
              <div className="mb-3 row">
                <label htmlFor="metricsNamespaceName" className="col-sm-2 col-form-label">
                  Name
                </label>
                <div className="col-sm-10">
                  <InputText
                    type="text"
                    className="form-control"
                    id="metricsNamespaceName"
                    disabled={selectMetricsNamespace.namespace.namespace_id != null}
                    defaultValue={selectMetricsNamespace.namespace.name}
                    onChange={onChangeName}
                  />
                </div>
              </div>
              <div className="mb-3 row">
                <label htmlFor="metricsNamespaceWeight" className="col-sm-2 col-form-label">
                  Weight
                </label>
                <div className="col-sm-10 d-flex flex-row">
                  <InputText
                    type="number"
                    min={0}
                    max={1000}
                    step={0.01}
                    className="form-control"
                    id="metricsNamespaceWeight"
                    value={selectMetricsNamespace.namespace.weight.toString()}
                    onChange={onChangeWeight}
                  />
                  <div className="col-form-label ms-2" style={{ width: 80 }}>
                    [{selectMetricsNamespaceWeightPercent}%]
                  </div>
                </div>
              </div>
              <div className="mb-3 d-flex flex-row justify-content-end">
                <button
                  type="submit"
                  className="btn btn-outline-primary ms-1 text-nowrap"
                  disabled={!selectMetricsNamespace.namespace.name || saveLoader}
                >
                  {saveLoader && (
                    <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                  )}
                  {typeof selectMetricsNamespace.namespace.namespace_id === 'undefined' ? 'Create' : 'Save'}
                </button>
                <button
                  type="button"
                  className="btn btn-outline-primary ms-1 text-nowrap"
                  onClick={onCancelMetricsNamespace}
                >
                  Cancel
                </button>
                {selectMetricsNamespace.namespace.namespace_id != null &&
                  selectMetricsNamespace.namespace.namespace_id > 0 && (
                    <button
                      type="button"
                      className="btn btn-outline-danger ms-1 text-nowrap"
                      onClick={onRemoveMetricsNamespace}
                      disabled={!selectMetricsNamespace.namespace.name || saveLoader}
                    >
                      {saveLoader && (
                        <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                      )}
                      {selectMetricsNamespace.namespace.disable ? 'Restore' : 'Remove'}
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

export default NamespacePage;
